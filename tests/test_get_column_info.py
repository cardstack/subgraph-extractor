import pytest
from subgraph_extractor.cli import *
from pytest_postgresql import factories
import pandas

postgresql_my_proc = factories.postgresql_proc(load=["tests/resources/example_db.sql"])
sample_subgraph = factories.postgresql("postgresql_my_proc")


@pytest.fixture
def db_conn(sample_subgraph):
    return f"postgresql://{sample_subgraph.info.user}:@{sample_subgraph.info.host}:{sample_subgraph.info.port}/{sample_subgraph.info.dbname}"


@pytest.fixture
def table_schema_name(db_conn):
    return get_subgraph_table_schema("my_test_subgraph", db_conn)


@pytest.fixture
def valid_table_name():
    return "sample_table"


def test_get_subgraph_table_schemas(db_conn):
    subgraph_schemas = get_subgraph_table_schemas(db_conn)
    assert subgraph_schemas == {
        "my_test_subgraph": {
            "id": "internalversion1",
            "label": "my_test_subgraph",
            "subgraph_deployment": "SUBGRAPHIPFS",
            "subgraph_table_schema": "sgd1",
        }
    }


def test_get_subgraph_table_schema(db_conn):
    subgraph_schema = get_subgraph_table_schema("my_test_subgraph", db_conn)
    assert subgraph_schema == "sgd1"


def test_get_column_types(db_conn, table_schema_name, valid_table_name):
    db_columns = get_column_types(db_conn, table_schema_name, valid_table_name)
    assert db_columns == {
        "block_number": "numeric",
        "amount": "numeric",
        "from_address": "text",
        "to_address": "text",
    }
