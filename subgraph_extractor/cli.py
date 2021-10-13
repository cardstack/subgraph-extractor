import os

import click
import pandas


def get_select_all_exclusive(
    database_string, table_schema, table_name, ignored_columns=[]
):
    """Returns a select all statement excluding certain columns"""
    ignored_column_string = ", ".join(
        f"'{column_name}'" for column_name in ignored_columns
    )

    return pandas.read_sql(
        sql=f"""SELECT 'SELECT ' || STRING_AGG('"' || column_name || '"', ', ') || ' FROM {table_schema}.{table_name}'
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            AND table_schema = '{table_schema}'
            AND column_name NOT IN ({ignored_column_string})
            """,
        con=database_string,
    ).iloc[0][0]


def get_tables_in_schema(database_string, table_schema, ignored_tables=[]):
    all_tables = pandas.read_sql(
        f"""
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema = '{table_schema}'
    """,
        con=database_string,
    )["table_name"].tolist()
    return list(set(all_tables) - set(ignored_tables))


def get_subgraph_schemas(database_string):
    return pandas.read_sql(
        f"""
    SELECT subgraph as subgraph_id, name as table_schema from deployment_schemas
    """,
        con=database_string,
    ).to_dict("records")


@click.command()
@click.option(
    "--database-string",
    default="postgresql://graph-node:let-me-in@localhost:5432/graph-node",
    help="The database string to connect to",
)
def main(database_string):
    """Connects to your database and pulls all data from all subgraphs"""

    # TODO: find the partitions to use
    for subgraph in get_subgraph_schemas(database_string):
        table_schema = subgraph["table_schema"]
        subgraph_id = subgraph["subgraph_id"]
        # We can ignore this auto-created table, the others should all be
        # explicitly created mappings
        tables = get_tables_in_schema(database_string, table_schema, ["poi2$"])
        # We need to get the column names and have to explicity exclude some
        for table_name in tables:
            df = pandas.read_sql(
                sql=get_select_all_exclusive(
                    database_string, table_schema, table_name, ["vid", "block_range"]
                ),
                con=database_string,
                coerce_float=False,
            )
            basedir = os.path.join(
                "data", f"subgraph={subgraph_id}", f"table={table_name}"
            )
            os.makedirs(basedir, exist_ok=True)
            df.to_parquet(os.path.join(basedir, "data.parquet"))


if __name__ == "__main__":
    main()