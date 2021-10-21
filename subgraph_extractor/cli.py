import os

import click
import pandas
import yaml

TYPE_MAPPINGS = {
    'numeric': 'bytes',
    #'bytea': 'bytes',
    'text': 'string'
}

def get_select_all_exclusive(
    database_string, table_schema, table_name, partition_column, start_partition=None, end_partition=None, ignored_columns=[]
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
    for row in df.to_dict('records'):
        column_types[row['column_name']] = row['data_type']
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
@click.option("--subgraph-config", type=click.File('r'), help="The config file specifying the data to extract")
@click.option(
    "--database-string",
    default="postgresql://graph-node:let-me-in@localhost:5432/graph-node",
    help="The database string to connect to",
)
def main(subgraph_config, database_string):
    """Connects to your database and pulls all data from all subgraphs"""

    config = yaml.safe_load(subgraph_config)
    subgraph = get_subgraph_schema(config['id'], database_string)
    print(subgraph, config)
    table_schema = subgraph["table_schema"]
    subgraph_id = subgraph["subgraph_id"]
    for table_name, table_config in config['tables'].items():
        partition_column = table_config['partition']
        limits_df = pandas.read_sql(
            sql=f"select min({partition_column}) as min_partition, max({partition_column}) as max_partition from {table_schema}.{table_name}",
            con=database_string,
            coerce_float=False)
        print(limits_df)
        min_partition= int(limits_df['min_partition'].iloc[0])
        max_partition = int(limits_df['max_partition'].iloc[0])
        for partition_size in table_config['partition_sizes']:
            print(partition_size)
            # Floor the ranges
            start_partition_allowed = (min_partition // partition_size)*partition_size
            end_partition_allowed = (max_partition // partition_size)*partition_size
            print(start_partition_allowed, end_partition_allowed)
            for start_partition in range(start_partition_allowed, end_partition_allowed, partition_size):
                end_partition = start_partition + partition_size
                df = pandas.read_sql(
                    sql=get_select_all_exclusive(
                        database_string, table_schema, table_name,
                        partition_column,
                        start_partition=start_partition,
                        end_partition=end_partition,
                        ignored_columns=["vid", "block_range"]
                    ),
                    con=database_string,
                    coerce_float=False,
                )
                basedir = os.path.join(
                    "data", f"subgraph={subgraph_id}", f"table={table_name}",
                    f"partition_size={partition_size}",
                    f"start_partition={start_partition}",
                    f"end_partition={end_partition}",
                )
                os.makedirs(basedir, exist_ok=True)
                # Get the column types
                database_types = get_column_types(database_string, table_schema, table_name)
                print(database_types)
                print(df.dtypes[0])
                update_types = {}
                for column_name in df.columns:
                    database_type = database_types[column_name]
                    if database_type in TYPE_MAPPINGS:
                        update_types[column_name] = TYPE_MAPPINGS[database_type]
                print(update_types)
                typed_df = df.astype(update_types)
                print(typed_df.dtypes)
                typed_df.to_parquet(os.path.join(basedir, "data.parquet"))


if __name__ == "__main__":
    main()
