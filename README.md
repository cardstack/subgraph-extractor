# subgraph-extractor
Extracts data from the database for a graph-node and stores it in parquet files

# Installation

For developing, it's recommended to use conda to create an environment.

Create one with python 3.9

    conda create --name subgraph-extractor python=3.9

Now activate it

    conda activate subgraph-extractor

Install the dev packages (note there is no space after the `.`)

    pip install -e .[dev]

# Use

Now you can use the main entrypoint, see help for more details

    subgraph_extractor --help

# Contributing

Please format everything with black and isort

    black . && isort --profile=black .

