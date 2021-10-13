import click


@click.command()
@click.option(
    "--database-string",
    default="postgresql://graph-node:let-me-in@localhost:5432/graph-node",
    help="The database string to connect to",
)
def main(database_string):
    """Connects to your database and pulls all data from all subgraphs"""
    click.echo("Hi!")


if __name__ == "__main__":
    main()
