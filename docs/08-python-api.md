Python API
==========

The Python API allows users to query Sleeper from Python, and to trigger uploads of data in Parquet files. There is
also the ability to upload records directly from the Python API, but this is only intended to be used for very small
volumes of data.

## Requirements

* Python 3.7+

## Installation

From the `python` directory, run:
```bash
pip install .
```

## Known issues

* Python (pyarrow) uses INT64 in saved Parquet files, so the Sleeper schema must use LongType, not IntType for
integer columns.

## Functions

### SleeperClient(basename: str)

This is the initialiser for the SleeperClient class. This class is also how the user will run the ingest and query functions.

* basename: This is the Sleeper instance id.

#### Example:

```python
from sleeper.sleeper import SleeperClient
my_sleeper = SleeperClient('my_sleeper_instance')
```

### write_single_batch(table_name: str, records_to_write: list)

Write a batch of records into Sleeper. As noted above, this is not intended to be used for large volumes of
data.

* `table_name`: This is the name of the table to ingest data to.
* `records_to_write`: This should be a list of dictionaries to write to Sleeper. Each dictionary should contain a single record.

#### Example:

```python
records = [{'key': 'my_key', 'value': 'my_value'}, {'key': 'my_key2', 'value': 'my_value2'}]
my_sleeper.write_single_batch('my_table', records)
```

### ingest_parquet_files_from_s3(table_name: str, files: list):

Ingest data from Parquet files in S3 into the given table. Note that Sleeper must have been given permission
to read files in that bucket. This can be done by specifying the `sleeper.ingest.source.bucket` parameter
in the instance properties file.

* `table_name`: This is the name of the table to ingest data to
* `files`: This should be a list of files or directories in S3, in the form `bucket/file`. If directories are specified
then all Parquet files contained in them will be ingested.

#### Example:

```python
files = ["my_bucket/a_directory/", "my_bucket/a_file"]
my_sleeper.ingest_parquet_files_from_s3('my_table', files)
```

### bulk_import_parquet_files_from_s3(table_name: str, files: list, id: str = str(uuid.uuid4()), platform_spec: dict = None):

Bulk imports the data from Parquet files in S3 into the given table. Note that Sleeper must have been given
permission to read files in that bucket. This can be done by specifying the `sleeper.ingest.source.bucket parameter`
in the instance properties file.

* `table_name`: This is the name of the table to ingest data to
* `files`: This should be a list of files or directories in S3, in the form `bucket/file`. If directories are specified
then all Parquet files contained in them will be ingested
* `id`: This is the id of the bulk import job. This id will appear in the name of the cluster that runs the job. If
no id is provided a random one will be generated. Note that only lower case letters, numbers and dashes should be used.
* `platform_spec`: This optional parameter allows you to configure details of the EMR cluster that is created to run
the bulk import job. This should be a dict, containing parameters specifying details of the cluster (see the second
example below). If this is not provided then sensible defaults are used.

#### Example:

```python
files = ["my_bucket/a_directory/", "my_bucket/a_file"]

my_sleeper.bulk_import_parquet_files_from_s3('my_table', files, 'mybulkimportjob')

platform_spec = {
    "sleeper.table.bulk.import.emr.executor.initial.instances": "1",
    "sleeper.table.bulk.import.emr.executor.max.instances": "10",
    "sleeper.table.bulk.import.emr.release.label": "emr-6.10.0",
    "sleeper.table.bulk.import.emr.master.x86.instance.type": "m6i.xlarge",
    "sleeper.table.bulk.import.emr.executor.x86.instance.type": "m6i.4xlarge",
    "sleeper.table.bulk.import.emr.executor.market.type": "SPOT" // Use "ON_DEMAND" for on demand instances
}

my_sleeper.bulk_import_parquet_files_from_s3('my_table', files, 'my_bulk_import_job', platform_spec)
```

### exact_key_query(table_name:str, keys: list)

Allows for the querying for records matching a specific key from Sleeper.

* `table_name`: This is the name of the table to query
* `queried_key`: This should be the key or keys to query Sleeper for in the form of a list of dicts
* `key_schema_name`: This should be the name given to keys as it appears in their Sleeper instance schema

This function returns a list of the records that contain the queried key. Each element of this list is another list
containing two tuples, one containing the schema name of the key followed by they key (the one that was queried) and
the other tuple contains the associated value.

#### Example:

```python
my_sleeper.exact_key_query('my_table', {"key": ["akey", "anotherkey", "yetanotherkey"]})
// An equivalent form
my_sleeper.exact_key_query('my_table', [{"key": "akey"}, {"key": "anotherkey"}, {"key": "yetanotherkey"}])
```

And this would return something along the lines of 

```python
[[('key', 'akey'), ('value', 'my_value')]]
```

In this example, there was one record found with the key `akey` which has the value of `my_value`. If there
were more records with this key then the returned list would be longer.

### range_key_query(table_name: str, regions: list)

Queries for all records where the key is in a given range, for example between 'a' and 'c'.

* `table_name`: This is the name of the table to query
* `regions`: A list of regions where each region is a dict with a key of the name of the row key field and
the value is a list, either ["a", "c"] or ["a", True, "b", True] where the latter form allows the user to
specify whether the ends are inclusive (True means inclusive, the default is that the minimum is inclusive
and the maximum is exclusive)
* `queried_key_min`: This should be the lower bound of the range to query for
* `queried_key_max`: This should be the upper bound of the range to query for
* `key_schema_name`: This should be the name given to keys as it appears in their Sleeper instance schema.

#### Example function call

```python
my_sleeper.range_key_query('my_table', [ {"key": ["a", True, "c", False]} ])
```

And this would return something similar to

```python
[[('key': 'a'), ('value': 'first_key')], [('key': 'b1'), ('value': 'second_key')]. [('key': 'b2'), ('value': 'third_key')]]
```

### create_batch_writer(table_name)

Creates a writer for writing batches of records to Sleeper.

* `table_name`: This is the name of the table to ingest data to.
  
The returned object is designed to be used within a Python context manager
as follows:

```python
with my_sleeper.create_batch_writer('my_table_name') as writer:
    ...
    records = [ .... ]
    ...
    writer.write(records)
    ...
    # Write some more records
    more_records = ...

    writer.write(more_records)
```

See the code examples for a working example.

## Examples

More examples can be found in the `examples` directory.

* `simplest_example.py` - A simple demonstration of interacting with Sleeper.
* `large_writes.py` - Example showing how to write large batches of data to Sleeper.
