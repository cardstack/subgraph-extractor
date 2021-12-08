import os
from datetime import datetime

import click
import numpy as np
import pandas
import pyarrow
import pyarrow.parquet as pq
import yaml
from cloudpathlib import AnyPath
from deepdiff import DeepDiff
from simple_term_menu import TerminalMenu
from tqdm import tqdm

TYPE_MAPPINGS = {"numeric": "bytes", "text": "string", "boolean": "bool"}


def get_select_all_exclusive(
    database_string,
    subgraph_table_schema,
    table_name,
    partition_column,
    start_partition,
    end_partition,
):

    return pandas.read_sql(
        sql=f"""SELECT 
                'SELECT ' || STRING_AGG('"' || column_name || '"', ', ') || ' 
                    FROM {subgraph_table_schema}.{table_name}
                    WHERE
                        {partition_column} >= %(start_partition)s
                        AND {partition_column} < %(end_partition)s
                    ORDER BY {partition_column}'
            FROM information_schema.columns
            WHERE table_name = %(table_name)s
            AND table_schema = %(subgraph_table_schema)s
            AND column_name NOT IN ('vid', 'block_range')
            """,
        params={
            "start_partition": start_partition,
            "end_partition": end_partition,
            "table_name": table_name,
            "subgraph_table_schema": subgraph_table_schema,
        },
        con=database_string,
    ).iloc[0][0]


def get_column_types(database_string, subgraph_table_schema, table_name):
    df = pandas.read_sql(
        sql=f"""SELECT column_name, data_type
                FROM information_schema.columns
            WHERE table_name = %(table_name)s
            AND table_schema = %(subgraph_table_schema)s
            """,
        params={
            "table_name": table_name,
            "subgraph_table_schema": subgraph_table_schema,
        },
        con=database_string,
    )
    return {row["column_name"]: row["data_type"] for row in df.to_dict("records")}


def get_subgraph_table_schemas(database_string):
    schema_data = pandas.read_sql(
        """
    SELECT
  ds.subgraph AS subgraph_deployment,
  ds.name AS subgraph_table_schema,
  sv.id,
  s.name as label
FROM
  deployment_schemas ds
  LEFT JOIN subgraphs.subgraph_version sv ON (ds.subgraph = sv.deployment)
  LEFT JOIN subgraphs.subgraph s ON (s.current_version = sv.id)
WHERE
  ds.active AND s.current_version is not NULL
    """,
        con=database_string,
    ).to_dict("records")
    return {subgraph["label"]: subgraph for subgraph in schema_data}


def get_subgraph_table_schema(subgraph, database_string):
    schemas = get_subgraph_table_schemas(database_string)
    return schemas[subgraph]["subgraph_table_schema"]


def get_subgraph_deployment(subgraph, database_string):
    schemas = get_subgraph_table_schemas(database_string)
    return schemas[subgraph]["subgraph_deployment"]


def convert_columns(df, database_types, table_config):
    update_types = {}
    new_columns = {}
    for column, mappings in table_config["column_mappings"].items():
        for new_column_name, new_column_config in mappings.items():
            scale_factor = new_column_config.get("downscale")
            if scale_factor:
                new_column = df[column] // scale_factor
            else:
                new_column = df[column]
            if new_column_config.get("max_value"):
                max_value = new_column_config["max_value"]
                validity_column = new_column_config["validity_column"]
                default = new_column_config["default"]
                new_columns[new_column_name] = np.where(
                    new_column <= max_value, new_column, default
                )
                new_columns[validity_column] = np.where(
                    new_column <= max_value, True, False
                )
                update_types[validity_column] = "boolean"
            else:
                new_columns[new_column_name] = new_column
            update_types[new_column_name] = new_column_config["type"]
    for column_name in df.columns:
        database_type = database_types[column_name]
        if database_type in TYPE_MAPPINGS:
            update_types[column_name] = TYPE_MAPPINGS[database_type]
        if database_type == "numeric":
            df[column_name] = df[column_name].map(
                lambda x: int(x).to_bytes(32, byteorder="big")
            )
    df = df.rename_axis(None)
    df = df.assign(**new_columns)
    table = pyarrow.Table.from_pandas(df, preserve_index=False)
    schema = table.schema
    types = {
        "uint64": pyarrow.uint64(),
        "bytes": pyarrow.binary(),
        "bool": pyarrow.bool_(),
        "boolean": pyarrow.bool_(),
        "string": pyarrow.string(),
        "Numeric38": pyarrow.decimal128(precision=38),
    }
    for column_name, new_type in update_types.items():
        field_index = schema.get_field_index(column_name)
        field = schema.field(field_index)
        new_field = field.with_type(types[new_type])
        schema = schema.set(field_index, new_field)

    table = table.cast(schema, safe=False)
    return table


def get_partition_iterator(min_partition, max_partition, partition_sizes):
    for partition_size in sorted(partition_sizes, reverse=True):
        start_partition_allowed = (min_partition // partition_size) * partition_size
        end_partition_allowed = (max_partition // partition_size) * partition_size
        last_max_partition = None
        for start_partition in range(
            start_partition_allowed, end_partition_allowed, partition_size
        ):
            last_max_partition = start_partition + partition_size
            yield partition_size, start_partition, start_partition + partition_size
        if last_max_partition is not None:
            min_partition = last_max_partition


def get_partitions(
    database_string,
    partition_column,
    partition_sizes,
    subgraph_table_schema,
    table_name,
):
    limits_df = pandas.read_sql(
        sql=f"select min({partition_column}) as min_partition, max({partition_column}) as max_partition from {subgraph_table_schema}.{table_name}",
        con=database_string,
        coerce_float=False,
    )
    min_partition = int(limits_df["min_partition"].iloc[0])
    max_partition = int(limits_df["max_partition"].iloc[0])
    yield from get_partition_iterator(min_partition, max_partition, partition_sizes)


def get_partition_file_location(
    table_dir, partition_size, start_partition, end_partition
):
    return table_dir.joinpath(
        f"partition_size={partition_size}",
        f"start_partition={start_partition}",
        f"end_partition={end_partition}",
        "data.parquet",
    )


def filter_existing_partitions(table_dir, partitions):
    # Iterate in reverse until one exists, assume all previous exist
    # We must iterate forwards for processing so return in the correct order.
    new_partitions = []
    for partition in sorted(partitions, reverse=True, key=lambda x: x[1]):
        if get_partition_file_location(table_dir, *partition).exists():
            return sorted(new_partitions, reverse=True)
        else:
            new_partitions.append(partition)
    return new_partitions


def extract_from_config(subgraph_config, database_string, output_location):
    """Connects to your database and pulls all data from all subgraphs"""

    config = yaml.safe_load(AnyPath(subgraph_config).open("r"))
    subgraph = config["subgraph"]
    subgraph_deployment = get_subgraph_deployment(subgraph, database_string)
    subgraph_table_schema = get_subgraph_table_schema(subgraph, database_string)
    root_output_location = AnyPath(output_location).joinpath(
        config["name"], config["version"]
    )
    config_output_location = root_output_location.joinpath("config.yaml")
    if config_output_location.exists():
        existing_config = yaml.safe_load(config_output_location.open("r"))
        config_difference = DeepDiff(existing_config, config)
        if config_difference:
            raise Exception(
                f"Config provided does not match the previously seen version at {config_output_location}"
            )
    else:
        config_output_location.parent.mkdir(parents=True, exist_ok=True)
        with config_output_location.open("w") as f_out:
            yaml.dump(config, f_out)
    for table_name, table_config in tqdm(
        config["tables"].items(), leave=False, desc="Tables"
    ):
        table_dir = root_output_location.joinpath(
            "data", f"subgraph={subgraph_deployment}", f"table={table_name}"
        )
        partition_column = table_config["partition_column"]
        partition_range = get_partitions(
            database_string,
            partition_column,
            table_config["partition_sizes"],
            subgraph_table_schema,
            table_name,
        )
        database_types = get_column_types(
            database_string, subgraph_table_schema, table_name
        )
        unexported_partitions = filter_existing_partitions(table_dir, partition_range)
        for partition_size, start_partition, end_partition in tqdm(
            unexported_partitions, leave=False, desc="Paritions"
        ):
            filepath = get_partition_file_location(
                table_dir, partition_size, start_partition, end_partition
            )
            if not filepath.exists():
                df = pandas.read_sql(
                    sql=get_select_all_exclusive(
                        database_string,
                        subgraph_table_schema,
                        table_name,
                        partition_column,
                        start_partition=start_partition,
                        end_partition=end_partition,
                    ),
                    con=database_string,
                    coerce_float=False,
                )
                typed_df = convert_columns(df, database_types, table_config)
                filepath.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(typed_df, filepath)
    with root_output_location.joinpath("latest.yaml").open("w") as f_out:
        yaml.dump(
            {
                "subgraph": subgraph,
                "subgraph_deployment": subgraph_deployment,
                "updated": datetime.now(),
            },
            f_out,
        )


@click.command()
@click.option(
    "--subgraph-config",
    help="The config file specifying the data to extract",
    required=True,
)
@click.option(
    "--database-string",
    default="postgresql://graph-node:let-me-in@localhost:5432/graph-node",
    help="The database string for connections, defaults to a local graph-node",
)
@click.option(
    "--output-location",
    default="data",
    help="The base output location, whether local or cloud",
)
def main(subgraph_config, database_string, output_location):
    """Connects to your database and pulls all data from all subgraphs"""
    extract_from_config(subgraph_config, database_string, output_location)

def get_tables_in_schema(database_string, subgraph_table_schema, ignored_tables=[]):
    all_tables = pandas.read_sql(
        f"""
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema = '{subgraph_table_schema}'
    """,
        con=database_string,
    )["table_name"].tolist()
    return sorted(list(set(all_tables) - set(ignored_tables)))


@click.command()
@click.option(
    "--config-location", help="The output file location for this config", required=True
)
@click.option(
    "--database-string",
    default="postgresql://graph-node:let-me-in@localhost:5432/graph-node",
    help="The database string for connections, defaults to a local graph-node",
)
def config_generator(config_location, database_string):
    # Minimise the width any particular column can use in the preview
    pandas.set_option("display.max_colwidth", 8)
    # Let pandas figure out the width of the terminal
    pandas.set_option("display.width", None)

    config = {"name": "test_config", "version": "0.0.1"}

    subgraph_table_schemas = get_subgraph_table_schemas(database_string)

    def preview_schema_data(label):
        schema = subgraph_table_schemas[label]
        table_spacer = "\n - "
        table_list = get_tables_in_schema(
            database_string, schema["subgraph_table_schema"]
        )
        table_list_formatted = table_spacer + table_spacer.join(table_list)
        # Make nicer
        return f"""
Subgraph: {schema["subgraph_deployment"]}
Tables ({len(table_list)}): {table_list_formatted}
"""

    options = list(subgraph_table_schemas.keys())
    terminal_menu = TerminalMenu(
        options,
        title="Please select the subgraph you want to extract",
        preview_command=preview_schema_data,
        preview_size=0.75,
    )
    menu_entry_index = terminal_menu.show()
    schema_data = subgraph_table_schemas[options[menu_entry_index]]
    subgraph_table_schema = schema_data["subgraph_table_schema"]
    config["subgraph"] = schema_data["label"]

    tables = get_tables_in_schema(database_string, subgraph_table_schema)

    def preview_table_data(table):
        subset = pandas.read_sql(
            f"select * from {subgraph_table_schema}.{table} limit 10",
            con=database_string,
        )
        return str(subset.head())

    terminal_menu = TerminalMenu(
        tables,
        title="Please select the tables you want to extract",
        preview_command=preview_table_data,
        preview_size=0.75,
        multi_select=True,
    )
    table_entry_index = terminal_menu.show()
    selected_tables = [tables[index] for index in table_entry_index]

    config["tables"] = {}
    for table in selected_tables:
        table_config = {}
        column_types = get_column_types(database_string, subgraph_table_schema, table)
        column_names = sorted(list(column_types.keys()))
        terminal_menu = TerminalMenu(
            column_names, title=f"Please select the partition column for table {table}"
        )
        partition_index = terminal_menu.show()
        partition_column = column_names[partition_index]
        table_config["partition_column"] = partition_column
        table_config["partition_sizes"] = [32768]

        numeric_columns = sorted(
            [
                column
                for column, data_type in column_types.items()
                if data_type == "numeric"
            ]
        )

        if len(numeric_columns) > 0:
            terminal_menu = TerminalMenu(
                numeric_columns,
                title=f"These columns are numeric and will be exported as bytes unless they are mapped, which should be mapped to another type?",
                multi_select=True,
            )
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
                            "validity_column": f"{column}_uint64_valid",
                        }
                    }
        config["tables"][table] = table_config
    with open(config_location, "w") as f_out:
        yaml.dump(config, f_out)
