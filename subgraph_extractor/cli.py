from datetime import datetime

import click
import numpy as np
import pandas
import pyarrow
import pyarrow.parquet as pq
import yaml
from cloudpathlib import AnyPath, CloudPath
from deepdiff import DeepDiff
from simple_term_menu import TerminalMenu
from tqdm import tqdm
import tempfile

TYPE_MAPPINGS = {"numeric": "bytes", "text": "string", "boolean": "bool"}

BLOCK_COLUMN = "_block_number"
BLOCK_COLUMN_TYPE = pyarrow.uint32()


def get_select_all_exclusive(
    database_string,
    subgraph_table_schema,
    table_name,
    start_partition,
    end_partition,
):

    return pandas.read_sql(
        sql=f"""SELECT 
                'SELECT ' || STRING_AGG('"' || column_name || '"', ', ') || ', lower(block_range) as {BLOCK_COLUMN} 
                    FROM {subgraph_table_schema}.{table_name}
                    WHERE
                        lower(block_range) >= %(start_partition)s
                        AND lower(block_range) < %(end_partition)s
                    ORDER BY lower(block_range) asc'
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
  s.name as label,
  sd.earliest_ethereum_block_number::int as earliest_block,
  sd.latest_ethereum_block_number::int as latest_block
FROM
  deployment_schemas ds
  LEFT JOIN subgraphs.subgraph_version sv ON (ds.subgraph = sv.deployment)
  LEFT JOIN subgraphs.subgraph s ON (s.current_version = sv.id)
  LEFT JOIN subgraphs.subgraph_deployment sd ON (sd.deployment = ds.subgraph)
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


def get_subgraph_block_range(subgraph, database_string):
    schemas = get_subgraph_table_schemas(database_string)
    return schemas[subgraph]["earliest_block"], schemas[subgraph]["latest_block"]


def convert_columns(df, database_types, table_config):
    update_types = {BLOCK_COLUMN: BLOCK_COLUMN_TYPE}
    new_columns = {}
    for column, mappings in table_config.get("column_mappings", {}).items():
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
        if column_name != BLOCK_COLUMN:
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
        "uint32": pyarrow.uint32(),
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
    config = yaml.safe_load(AnyPath(subgraph_config).open("r"))
    return extract(config, database_string, output_location)


def write_config(config, root_output_location):
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


def extract(config, database_string, output_location):
    """Connects to your database and pulls all data from all subgraphs"""

    subgraph = config["subgraph"]
    subgraph_deployment = get_subgraph_deployment(subgraph, database_string)
    subgraph_table_schema = get_subgraph_table_schema(subgraph, database_string)
    earliest_block, latest_block = get_subgraph_block_range(subgraph, database_string)

    root_output_location = AnyPath(output_location).joinpath(
        config["name"], config["version"]
    )

    write_config(config, root_output_location)

    for table_name, table_config in tqdm(
        config["tables"].items(), leave=False, desc="Tables"
    ):
        table_dir = root_output_location.joinpath(
            "data", f"subgraph={subgraph_deployment}", f"table={table_name}"
        )
        partition_range = get_partition_iterator(
            earliest_block, latest_block, table_config["partition_sizes"]
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
            df = pandas.read_sql(
                sql=get_select_all_exclusive(
                    database_string,
                    subgraph_table_schema,
                    table_name,
                    start_partition=start_partition,
                    end_partition=end_partition,
                ),
                con=database_string,
                coerce_float=False,
            )
            typed_df = convert_columns(df, database_types, table_config)
            # Pyarrow can't take a file object so we have to write to a temp file
            # and upload directly
            filepath.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(filepath, CloudPath):
                with tempfile.TemporaryDirectory() as temp_dir:
                    pq_file_location = AnyPath(temp_dir).joinpath("data.parquet")
                    pq.write_table(typed_df, pq_file_location)
                    filepath.upload_from(pq_file_location)
            else:
                pq.write_table(typed_df, filepath)
    with root_output_location.joinpath("latest.yaml").open("w") as f_out:
        yaml.dump(
            {
                "subgraph": subgraph,
                "subgraph_deployment": subgraph_deployment,
                "updated": datetime.now(),
                "earliest_block": earliest_block,
                "latest_block": latest_block,
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
    """
    Connects to your database and pulls all data from the tables
    specified in the config file
    """
    extract_from_config(subgraph_config, database_string, output_location)


def get_tables_in_schema(database_string, subgraph_table_schema):
    """ "
    Returns a list of all tables in the schema which have a block_range column.
    This corresponds to tables which contain entities that we can extract
    """
    all_tables = pandas.read_sql(
        f"""
    SELECT distinct table_name FROM information_schema.columns 
    WHERE table_schema = '{subgraph_table_schema}'
    AND column_name = 'block_range'
    ORDER BY table_name
    """,
        con=database_string,
    )["table_name"].tolist()
    return all_tables


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

    config = {"name": AnyPath(config_location).stem, "version": "0.0.1"}

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
        # These sizes are just a sensible default for gnosis chain
        # With a block duration of about 5 seconds these correspond to (very roughly):
        # 1024 blocks = 1 1/2 hours
        # 1024*16 blocks = 1 day
        # 1024*128 blocks = 1 week
        # 1024*512 blocks = 1 month
        table_config["partition_sizes"] = [1024 * 512, 1024 * 128, 1024 * 16, 1024]

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
                title=f"These columns in table {table} are numeric and will be exported as bytes unless they are mapped, which should be mapped to another type?",
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
