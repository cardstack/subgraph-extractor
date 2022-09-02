from unittest import result
import pandas
import pytest
from pytest_postgresql import factories
import tempfile
from cloudpathlib import AnyPath
import pyarrow.dataset as ds
from pyarrow import fs
import shutil

from subgraph_extractor.cli import *

postgresql_my_proc = factories.postgresql_proc(load=["tests/resources/example_db.sql"])
db_conn = factories.postgresql("postgresql_my_proc")

CONFIG = {
            "name": "my_extract_name",
            "version": "0.0.1",
            "subgraph": "my_test_subgraph",
            "tables": {
                "prepaid_card_ask_sample": {
                    "partition_sizes": [524288,32768,1024]
                }
            }
        }

@pytest.fixture
def db_conn_string(db_conn):
    return f"postgresql://{db_conn.info.user}:@{db_conn.info.host}:{db_conn.info.port}/{db_conn.info.dbname}"


@pytest.fixture
def table_schema_name(db_conn_string):
    return get_subgraph_table_schema("my_test_subgraph", db_conn_string)


@pytest.fixture
def valid_table_name():
    return "sample_table"


def test_get_subgraph_table_schemas(db_conn_string):
    subgraph_schemas = get_subgraph_table_schemas(db_conn_string)
    assert subgraph_schemas == {
        "my_test_subgraph": {
            "id": "internalversion1",
            "label": "my_test_subgraph",
            "subgraph_deployment": "SUBGRAPHIPFS",
            "subgraph_table_schema": "sgd1",
            "latest_block": 19000000,
            "earliest_block": 18000000,
        }
    }


def test_get_subgraph_table_schema(db_conn_string):
    subgraph_schema = get_subgraph_table_schema("my_test_subgraph", db_conn_string)
    assert subgraph_schema == "sgd1"


def test_get_column_types(db_conn_string, table_schema_name, valid_table_name):
    db_columns = get_column_types(db_conn_string, table_schema_name, valid_table_name)
    assert db_columns == {
        "block_number": "numeric",
        "amount": "numeric",
        "from_address": "text",
        "to_address": "text",
    }


def test_blockrange_returns_uint32(db_conn_string):

    db_columns = get_column_types(db_conn_string, "sgd1", "prepaid_card_ask_sample")
    df = select_writable_in_range(
        db_conn_string,
        "sgd1",
        "prepaid_card_ask_sample",
        start_partition=18460372,
        end_partition=18888120,
    )
    typed_df = convert_columns(df, db_columns, {})
    assert typed_df.schema.field_by_name("_block_number").type == pyarrow.uint32()


def test_blockrange_returns_uint32_when_empty(db_conn_string):

    db_columns = get_column_types(db_conn_string, "sgd1", "prepaid_card_ask_sample")
    df = select_writable_in_range(
        db_conn_string,
        "sgd1",
        "prepaid_card_ask_sample",
        start_partition=19000000,
        end_partition=19100000,

    )
    typed_df = convert_columns(df, db_columns, {})
    assert typed_df.schema.field_by_name("_block_number").type == pyarrow.uint32()


def extract_to(output_folder, db_conn_string, db_conn=None, latest_block=None):
    """Extracts the database contents out, optionally only up to a specific block

    Args:
        output_folder (AnyPath): The folder to write to
        db_conn_string (str): the database connection string
        db_conn (): the database connection, required for overriding the latest block
        latest_block (int): override the latest block in the database
    """
    if latest_block is not None:
        cur = db_conn.cursor()
        cur.execute(f"UPDATE subgraphs.subgraph_deployment SET latest_ethereum_block_number={latest_block}")
        db_conn.commit()
        cur.close()
    extract(CONFIG, db_conn_string, output_folder)
    return output_folder.joinpath("my_extract_name", "0.0.1")

def get_dataset(output_folder):
    metadata_location = output_folder.joinpath("data", 
                                               "subgraph=SUBGRAPHIPFS",
                                               "table=prepaid_card_ask_sample",
                                               "_metadata")
    return ds.parquet_dataset(metadata_location, filesystem=fs.LocalFileSystem())

def test_write_out_results(db_conn_string):
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = AnyPath(temp_dir)
        resulting_folder = extract_to(temp_dir, db_conn_string)
        dataset = get_dataset(resulting_folder)
        df = dataset.to_table().to_pandas()
        assert len(df) == 6
        assert resulting_folder.joinpath("latest.yaml").exists()

def test_manually_constructing_partitons_matches_metadata_approach(db_conn_string):
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = AnyPath(temp_dir)
        resulting_folder = extract_to(temp_dir, db_conn_string)
        # Construct a pyarrow dataset from the metadata file
        metadata_dataset = get_dataset(resulting_folder)
        # Convert to a dataframe, sort to allow comparisons more easily
        metadata_df = metadata_dataset.to_table() \
                                      .to_pandas()\
                                      .sort_values(by="_block_number", ignore_index=True)
        
        # Construct a pyarrow dataset by calculating the required partitions and constructing filenames
        with open(resulting_folder.joinpath("latest.yaml")) as f_in:
            export_details = yaml.safe_load(f_in)
        earliest_block, latest_block = export_details["earliest_block"], export_details["latest_block"]
        partition_sizes = CONFIG["tables"]["prepaid_card_ask_sample"]["partition_sizes"]
        partitions = get_partitions(earliest_block, latest_block, partition_sizes)
        table_dir = resulting_folder.joinpath("data", 
                                              "subgraph=SUBGRAPHIPFS",
                                              "table=prepaid_card_ask_sample")
        files = [get_partition_file_location(table_dir, *partition) for partition in partitions] 
        partition_dataset = ds.dataset(source=files, filesystem=fs.LocalFileSystem())
        # Convert to a dataframe, sort to allow comparisons more easily
        partition_df = partition_dataset.to_table() \
                                        .to_pandas() \
                                        .sort_values(by="_block_number", ignore_index=True)
        # Check the two are identical
        assert len(metadata_df) == 6
        assert len(partition_df) == 6
        pandas.testing.assert_frame_equal(partition_df, metadata_df)

def test_writing_twice_when_block_increases_adds_data(db_conn_string, db_conn):
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = AnyPath(temp_dir)
        resulting_folder = extract_to(temp_dir, db_conn_string, db_conn, 18888000)
        dataset = get_dataset(resulting_folder)
        df = dataset.to_table().to_pandas()
        assert len(df) == 2
        assert resulting_folder.joinpath("latest.yaml").exists()
    
        # Move on some blocks and run again to check we're reading in the existing config and latest.yaml
        resulting_folder = extract_to(temp_dir, db_conn_string, db_conn, 19000000)
        dataset = get_dataset(resulting_folder)
        df = dataset.to_table().to_pandas()
        assert len(df) == 6
        assert resulting_folder.joinpath("latest.yaml").exists()


def test_second_run_fills_all_data_if_crashed_in_first(db_conn_string, db_conn):
    """
    This test requires some setup to generate a state seen when the following happens:
    * There has been at least one successful export
    * A later run writes out *some* of the files but crashes/stops part way through
    * The process is started again, and it should act as if the erroring run never happened

    An earlier version of the code looked at what had been written out to decide how many partitions
    needed to be written. It would encounter the *first* file a previous run had written out and
    assume that all *subsequent* files existed. This results in missing partitions.
    
    For a more detailed breakdown, see the PR this fix is introduced in:
    https://github.com/cardstack/subgraph-extractor/pull/2
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        # First we need a successful run
        temp_dir = AnyPath(temp_dir)
        resulting_folder = extract_to(temp_dir, db_conn_string, db_conn, 18888000)
        assert resulting_folder.joinpath("latest.yaml").exists()
        # We will need to reinstate this file so make a copy
        shutil.copy(resulting_folder.joinpath("latest.yaml"), resulting_folder.joinpath("latest.yaml-successful"))

    
        # Move on some blocks and run again,
        resulting_folder = extract_to(temp_dir, db_conn_string, db_conn, 19000000)

        # Break the state as it would be if not all files had been written
        missing_partition = resulting_folder.joinpath(
            "data",
            "subgraph=SUBGRAPHIPFS",
            "table=prepaid_card_ask_sample",
            "partition_size=1024",
            "start_partition=18977792",
            "end_partition=18978816"
        )

        shutil.rmtree(missing_partition)
        assert missing_partition.joinpath("data.parquet").exists() == False
        # Restore the first 'latest.yaml'
        shutil.copy(resulting_folder.joinpath("latest.yaml-successful"), resulting_folder.joinpath("latest.yaml"))

        # This is the run we are actually testing
        resulting_folder = extract_to(temp_dir, db_conn_string, db_conn, 19000000)
        # The file should be correctly written
        assert missing_partition.joinpath("data.parquet").exists()