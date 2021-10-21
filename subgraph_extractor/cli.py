import os

import click
import pandas
import yaml

import numpy as np

from simple_term_menu import TerminalMenu

TYPE_MAPPINGS = {"numeric": "bytes", "text": "string"}


def get_select_all_exclusive(
    database_string,
    table_schema,
    table_name,
    partition_column,
    start_partition=None,
    end_partition=None,
    ignored_columns=[],
):
    """Returns a select all statement excluding certain columns"""
    if len(ignored_columns) > 0:
        wrapped_columns = [f"'{column_name}'" for column_name in ignored_columns]
        ignored_column_string = f"AND column_name NOT IN ({', '.join(wrapped_columns)})"
    else:
        ignored_column_string = ""

    where_clauses = []
    if start_partition:
        where_clauses.append(f"{partition_column} >= {start_partition}")
    if end_partition:
        where_clauses.append(f"{partition_column} < {end_partition}")

    if len(where_clauses) > 0:
        where_clause = f"WHERE {' AND '.join(where_clauses)}"
    else:
        where_clause = ""

    return pandas.read_sql(
        sql=f"""SELECT 
                'SELECT ' || STRING_AGG('"' || column_name || '"', ', ') || ' 
                    FROM {table_schema}.{table_name}
                    {where_clause} 
                    ORDER BY {partition_column}'
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            AND table_schema = '{table_schema}'
            {ignored_column_string}
            """,
        con=database_string,
    ).iloc[0][0]


def get_column_types(database_string, table_schema, table_name):
    df = pandas.read_sql(
        sql=f"""SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                AND table_schema = '{table_schema}'
            """,
        con=database_string,
    )
    column_types = {}
    for row in df.to_dict("records"):
        column_types[row["column_name"]] = row["data_type"]
    return column_types


def get_subgraph_schema(subgraph_id, database_string):
    return pandas.read_sql(
        f"""
    SELECT subgraph as subgraph_id, name as table_schema from deployment_schemas
    where subgraph = %(subgraph_id)s
    """,
        params={"subgraph_id": subgraph_id},
        con=database_string,
    ).to_dict("records")[0]


@click.command()
@click.option(
    "--subgraph-config",
    type=click.File("r"),
    help="The config file specifying the data to extract",
    required=True
)
@click.option(
    "--database-string",
    default="postgresql://graph-node:let-me-in@localhost:5432/graph-node",
    help="The database string for connections, defaults to a local graph-node",
)
def main(subgraph_config, database_string):
    """Connects to your database and pulls all data from all subgraphs"""

    config = yaml.safe_load(subgraph_config)
    subgraph = get_subgraph_schema(config["id"], database_string)
    print(subgraph, config)
    table_schema = subgraph["table_schema"]
    subgraph_id = subgraph["subgraph_id"]
    for table_name, table_config in config["tables"].items():
        partition_column = table_config["partition"]
        limits_df = pandas.read_sql(
            sql=f"select min({partition_column}) as min_partition, max({partition_column}) as max_partition from {table_schema}.{table_name}",
            con=database_string,
            coerce_float=False,
        )
        print(limits_df)
        min_partition = int(limits_df["min_partition"].iloc[0])
        max_partition = int(limits_df["max_partition"].iloc[0])
        for partition_size in table_config["partition_sizes"]:
            print(partition_size)
            # Floor the ranges
            start_partition_allowed = (min_partition // partition_size) * partition_size
            end_partition_allowed = (max_partition // partition_size) * partition_size
            print(start_partition_allowed, end_partition_allowed)
            for start_partition in range(
                start_partition_allowed, end_partition_allowed, partition_size
            ):
                end_partition = start_partition + partition_size
                df = pandas.read_sql(
                    sql=get_select_all_exclusive(
                        database_string,
                        table_schema,
                        table_name,
                        partition_column,
                        start_partition=start_partition,
                        end_partition=end_partition,
                        ignored_columns=["vid", "block_range"],
                    ),
                    con=database_string,
                    coerce_float=False,
                )
                basedir = os.path.join(
                    "data",
                    f"subgraph={subgraph_id}",
                    f"table={table_name}",
                    f"partition_size={partition_size}",
                    f"start_partition={start_partition}",
                    f"end_partition={end_partition}",
                )
                os.makedirs(basedir, exist_ok=True)
                # Get the column types
                database_types = get_column_types(
                    database_string, table_schema, table_name
                )
                update_types = {}
                for column_name in df.columns:
                    database_type = database_types[column_name]
                    if database_type in TYPE_MAPPINGS:
                        update_types[column_name] = TYPE_MAPPINGS[database_type]
                new_column_settings = {}
                for column, mappings in table_config["column_mappings"].items():
                    for new_column_name, new_column_config in mappings.items():
                        if new_column_config.get("max_value"):
                            max_value = new_column_config["max_value"]
                            validity_column = new_column_config["validity_column"]
                            default = new_column_config["default"]
                            new_column_settings[new_column_name] = np.where(
                                df[column] <= max_value, df[column], default
                            )
                            new_column_settings[validity_column] = np.where(
                                df[column] <= max_value, True, False
                            )
                            update_types[validity_column] = "boolean"
                        else:
                            new_column_settings[new_column_name] = df[column]
                        update_types[new_column_name] = new_column_config["type"]

                df = df.assign(**new_column_settings)
                typed_df = df.astype(update_types)
                typed_df.to_parquet(os.path.join(basedir, "data.parquet"))



def get_tables_in_schema(database_string, table_schema, ignored_tables=[]):
    all_tables = pandas.read_sql(
        f"""
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema = '{table_schema}'
    """,
        con=database_string,
    )["table_name"].tolist()
    return sorted(list(set(all_tables) - set(ignored_tables)))


def get_subgraph_schemas(database_string):
    schema_lookup = {}
    schema_data = pandas.read_sql(
        f"""
    SELECT
  ds.subgraph AS subgraph_id,
  ds.name AS table_schema,
  sv.id,
  s.name as label
FROM
  deployment_schemas ds
  LEFT JOIN subgraphs.subgraph_version sv ON (ds.subgraph = sv.deployment)
  LEFT JOIN subgraphs.subgraph s ON (s.current_version = sv.id)
WHERE
  ds.active
    """,
        con=database_string,
    ).to_dict("records")
    for subgraph in schema_data:
        schema_lookup[subgraph['label']] = subgraph
    return schema_lookup



@click.command()
@click.option(
    "--config-location",
    help="The output file location for this config",
    required=True
)
@click.option(
    "--database-string",
    default="postgresql://graph-node:let-me-in@localhost:5432/graph-node",
    help="The database string for connections, defaults to a local graph-node",
)
def config_generator(config_location, database_string):
    # Minimise the width any particular column can use in the preview
    pandas.set_option('display.max_colwidth', 8)
    # Let pandas figure out the width of the terminal
    pandas.set_option('display.width', None)

    config = {}

    subgraph_schemas = get_subgraph_schemas(database_string)
    def preview_schema_data(label):
        schema = subgraph_schemas[label]
        table_spacer = "\n - "
        table_list = get_tables_in_schema(database_string, schema["table_schema"])
        table_list_formatted = table_spacer + table_spacer.join(table_list)
        # Make nicer
        return f"""
Subgraph: {schema["subgraph_id"]}
Tables ({len(table_list)}): {table_list_formatted}
"""
    options = list(subgraph_schemas.keys())
    terminal_menu = TerminalMenu(options, title="Please select the subgraph you want to extract", preview_command=preview_schema_data, preview_size=0.75)
    menu_entry_index = terminal_menu.show()
    schema_data = subgraph_schemas[options[menu_entry_index]]
    table_schema = schema_data["table_schema"]
    config["id"] = schema_data["subgraph_id"]

    tables = get_tables_in_schema(database_string, table_schema)

    def preview_table_data(table):
        subset = pandas.read_sql(f"select * from {table_schema}.{table} limit 10", con=database_string)
        return str(subset.head())
    terminal_menu = TerminalMenu(tables, title="Please select the tables you want to extract", preview_command=preview_table_data, preview_size=0.75, multi_select=True)
    table_entry_index = terminal_menu.show()
    selected_tables = [tables[index] for index in table_entry_index]


    def preview_column(label):
        schema = subgraph_schemas[label]
        table_spacer = "\n - "
        table_list = get_tables_in_schema(database_string, schema["table_schema"])
        table_list_formatted = table_spacer + table_spacer.join(table_list)
        # Make nicer
        return f"""
Subgraph: {schema["subgraph_id"]}
Tables ({len(table_list)}): {table_list_formatted}
"""

    config["tables"] = {}
    for table in selected_tables:
        table_config = {}
        column_types = get_column_types(database_string, table_schema, table)
        column_names = sorted(list(column_types.keys()))
        terminal_menu = TerminalMenu(column_names, title=f"Please select the partition column for table {table}")
        partition_index = terminal_menu.show()
        partition_column = column_names[partition_index]
        table_config["partition"] = partition_column
        table_config["partition_sizes"] = [1024]
        
        numeric_columns = sorted([column for column, data_type in column_types.items() if data_type == 'numeric'])

        if len(numeric_columns) > 0:
            terminal_menu = TerminalMenu(numeric_columns, title=f"These columns are numeric and will be exported as bytes unless they are mapped, which should be mapped to another type?", multi_select=True)
            mapped_indices = terminal_menu.show()
            selected_columns = [numeric_columns[index] for index in mapped_indices]
            if len(selected_columns) > 0:
                table_config["column_mappings"] = {}
                for column in selected_columns:
                    table_config["column_mappings"][column] = {
                        f"{column}_uint64": {
                            "type": "uint64",
                            "max_value": 0xFFFFFFFFFFFFFFFF,
                            "default": 0,
                            "validity_column": f"{column}_uint64_valid"
                        }
                    }
        config["tables"][table] = table_config
    with open(config_location, "w") as f_out:
        yaml.dump(config, f_out)