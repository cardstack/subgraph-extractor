import os

import click
import pandas

import sqlalchemy

def get_select_all_exclusive(
    database_string, table_schema, table_name, start_block=None, end_block=None, ignored_columns=[]
):
    """Returns a select all statement excluding certain columns"""
    if len(ignored_columns) > 0:
        wrapped_columns = [f"'{column_name}'" for column_name in ignored_columns]
        ignored_column_string = f"AND column_name NOT IN ({', '.join(wrapped_columns)})"
    else:
        ignored_column_string = ""
    
    where_clauses = []
    if start_block:
        where_clauses.append(f"block_number >= {start_block}")
    if end_block:
        where_clauses.append(f"block_number < {end_block}")
    
    if len(where_clauses) > 0:
        where_clause = f"WHERE {' AND '.join(where_clauses)}"
    else:
        where_clause = ""


    return pandas.read_sql(
        sql=f"""SELECT 
                'SELECT ' || STRING_AGG('"' || column_name || '"', ', ') || ' 
                    FROM {table_schema}.{table_name}
                    {where_clause} 
                    ORDER BY block_number'
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            AND table_schema = '{table_schema}'
            {ignored_column_string}
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

    for subgraph in get_subgraph_schemas(database_string):
        table_schema = subgraph["table_schema"]
        subgraph_id = subgraph["subgraph_id"]
        # We can ignore this auto-created table, the others should all be
        # explicitly created mappings
        tables = get_tables_in_schema(database_string, table_schema, ["poi2$"])
        # We need to get the column names and have to explicity exclude some
        for table_name in tables:
            # Assume all are workable by block like this
            # TODO: check for this column and do a timestamped export if not
            try:
                limits_df = pandas.read_sql(
                    sql=f"select min(block_number) as min_block, max(block_number) as max_block from {table_schema}.{table_name}",
                    con=database_string,
                    coerce_float=False)
                min_block = int(limits_df['min_block'].iloc[0])
                max_block = int(limits_df['max_block'].iloc[0])
                for granularity in [32**2, 32**3]:
                    # Floor the ranges
                    start_block_allowed = (min_block // granularity)*granularity
                    end_block_allowed = (max_block // granularity)*granularity
                    for start_block in range(start_block_allowed, end_block_allowed, granularity):
                        end_block = start_block + granularity
                        df = pandas.read_sql(
                            sql=get_select_all_exclusive(
                                database_string, table_schema, table_name,
                                start_block=start_block,
                                end_block=end_block,
                                ignored_columns=["vid", "block_range"]
                            ),
                            con=database_string,
                            coerce_float=False,
                        )
                        basedir = os.path.join(
                            "data", f"subgraph={subgraph_id}", f"table={table_name}",
                            f"granularity={granularity}",
                            f"start_block={start_block}",
                            f"end_block={end_block}",
                        )
                        os.makedirs(basedir, exist_ok=True)
                        df.to_parquet(os.path.join(basedir, "data.parquet"))
            except sqlalchemy.exc.ProgrammingError as e:
                print("skipping table", table_name, e)


if __name__ == "__main__":
    main()
