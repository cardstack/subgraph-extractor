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

## Creating a config files

The easiest way to start is to use the interactive subgraph config generator.

Start by launching the subgraph config generator with the location you want to write the config file to.

    subgraph_config_generator --config-location subgraph_config.yaml

It will default to using a local graph-node with default username & password (`postgresql://graph-node:let-me-in@localhost:5432/graph-node`) 
If you are connecting to something else you need to specify the database connection string with `--database-string`.

You will then be asked to select:

* The relevant subgraph
* From the subgraph, which tables to extract (multi-select)
* For each table, which column to partition on (this is typically the block number or timestamp)
* Any numeric columns that require mapping to another type \* see note below


### Numeric column mappings

Uint256 is a common data type in contracts but rare in most data processing tools.
The graph node creates a Postgres `Numeric` column for any field marked as a `BigInt` as it is capable of accurately storing uint256s (a common data type in solidity).

However, many downstream tools cannot handle these as numbers.

By default, these columns will be exported as bytes - a lossless representation but one that is not as usable for sums, averages, etc.
This is fine for some data, such as addresses or where the field is used to pack data (e.g. the tokenIds for decentraland).

For other use cases, the data must be converted to another type.
In the config file, you can specify numeric columns that need to be mapped to another type:

    column_mappings:
      my_original_column_name:
        my_new_column_name:
          type: uint64

However, if the conversion does not work (e.g. the number is too large), the extraction will stop with an error.
This is fine for cases where you know the range (e.g. timestamp or block number).
For other cases you can specify a maximum value, default and a column to store whether the row was at most the maximum value:

    column_mappings:
      my_original_column_name:
        my_new_column_name:
          type: uint64
          max_value: 18446744073709551615
          default: 0
          validity_column: new_new_column_name_valid

If the number is over 18446744073709551615, there will be a 0 stored in the column `my_new_column_name`  and `FALSE` stored in `new_new_column_name_valid`.

If your numbers are too large but can be safely lowered for your usecase (e.g. converting from wei to gwei) you can provide a `downscale` value:

    column_mappings:
      transfer_fee_wei:
        transfer_fee_gwei:
          downscale: 1000000000
          type: uint64
          max_value: 18446744073709551615
          default: 0
          validity_column: transfer_fee_gwei_valid

This will perform an integer division (divide and floor) the original value. **WARNING** this is a lossy conversion.

You may have as many mappings for a single column as you want, and the original will always be present as bytes.

The following numeric types are allowed:

* int8, int16, int32, int64
* uint8, uint16, uint32, uint64
* float32, float64
* Numeric38 (this is a numeric/Decimal column with 38 digits of precision)

# Contributing

Please format everything with black and isort

    black . && isort --profile=black .

