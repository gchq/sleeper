Tables
======

A Sleeper instance contains one or more tables. Each table must have a name and schema. A table also has a state store
for storing metadata about the table, and it can be taken offline to disable certain background operations.

All resources for the instance, such as the S3 bucket used for storing data in a table, ECS clusters and lambda
functions are shared across all the tables.

## The state store

Each table has metadata associated to it. This metadata is stored in a state store and consists of information about
files that are in the system, and the partitions. See the [design documentation](../design.md#State_store) for more
information.

The implementation of this can be chosen in the table property `sleeper.table.statestore.classname`, but usually this
should be left as the default value.

## Design data processing

Sleeper can apply processing to table data such that all data in the table is seen to have that processing pre-applied.
For example, this can be used to combine rows with the same values for row keys and sort keys, or to age off old data.
See more information on this in the [data processing document](data-processing.md).

## Add/edit a table

Scripts can be used to add, rename and delete tables in a Sleeper instance. If using the scripts, creating a new table
will consist of the following steps:

1. Use the `estimateSplitPoints.sh` script to estimate split points from your data.
2. Use the `addTable.sh` script to create the table.
3. Use the `reinitialiseTable.sh` script with the split points from the first step.
4. Use the `sendToIngestBatcher.sh` script to send your data to the ingest batcher to be added to the table.

All of these scripts will rely on a schema for your table, which should be created first.
See [creating a schema](schema.md) for how to set up a schema for your table.

We also have scripts to rename and delete a table, and to take it offline / online. You can also edit table properties
with `adminClient.sh`.

Here's an example of how you might use these together to create and add data to a table:

```bash
cat ./scripts/templates/schema.template
{
  "rowKeyFields": [
    {
      "name": "key",
      "type": "StringType"
    }
  ],
  "valueFields": [
    {
      "name": "value",
      "type": "StringType"
    }
  ]
}
ID=my-instance-id
./scripts/utility/estimateSplitPoints.sh ./scripts/templates/schema.template 128 100000 32768 splits.file s3a://my-bucket/file.parquet
./scripts/utility/addTable.sh $ID table1
./scripts/utility/reinitialiseTable.sh $ID table1 true splits.file
./scripts/utility/sendToIngestBatcher.sh $ID table1 my-bucket/file.parquet
```

We'll look at the table scripts below. See the [ingest batcher documentation](ingest-batcher.md) for more information on
`sendToIngestBatcher.sh`.

### Pre-split partitions

Before you create a Sleeper table, it is worthwhile to pre-split partitions for the table. If you do not do this, your
state store will be initialised with a single root partition. Note that pre-splitting a table is important for any
large-scale use of Sleeper, and is essential for running bulk import jobs.

One way to do this is by taking a sample of your data to generate a split points file:

```bash
./scripts/utility/estimateSplitPoints.sh <schema-file> <num-partitions> <read-max-rows-per-file> <sketch-size> <output-split-points-file> <parquet-paths-as-separate-args>
```

The schema file should be the `schema.json` file you created for your table.

You can calculate the number of partitions by dividing the total number of rows you expect for your table by the average
number of rows you want per partition.

The estimate will be based on the given number of rows from the start of each input file. If your data is such that
the beginning of a file will not be representative of the distribution of row keys, you can either read more rows,
or prepare a representative sample first.

The sketch size controls the size and accuracy of the data sketches used to estimate the split points. It should be a
power of 2 greater than 2 and less than 65536. See the Apache DataSketches documentation for more information:

https://datasketches.apache.org/docs/Quantiles/ClassicQuantilesSketch.html

The paths to your sample data can be specified as a path in your local file system, or you can use the s3a:// scheme to
give a path in an S3 bucket like `s3a://my-bucket/my-prefix/file.parquet`.

You can apply the resulting split points when adding a table by setting an absolute path to the output file in the
table property `sleeper.table.splits.file`. If you've created a table but haven't added any data yet, you can apply a
change to this by reinitialising the table. In the future it will not be necessary to set this property when using the
instance configuration folder structure, see issue https://github.com/gchq/sleeper/issues/583.

### Add table

The `addTable.sh` script will create a new table with properties defined by a path provided to the script.
The template properies will be used as a basis and the provided script overriding where necessary.
Default table properites found in `templates/tableproperties.template`, and the schema defined in `templates/schema.template`.

```bash
cd scripts
editor templates/tableproperties.template
editor templates/schema.template
./utility/addTable.sh <instance-id> <table-name>
```

### Reinitialise a table

Reinitialising a table means deleting all its contents. This can sometimes be useful when you are experimenting
with Sleeper or if you created a table with the wrong schema.

You can reinitialise the table quickly by running the following command:

```bash
./scripts/utility/reinitialiseTable.sh <instance-id> <table-name> <optional-delete-partitions-true-or-false> <optional-split-points-file-location> <optional-split-points-file-base64-encoded-true-or-false>
```

For example

```bash
./scripts/utility/reinitialiseTable.sh sleeper-my-sleeper-config my-sleeper-table true /tmp/split-points.txt false
```

If you want to change the table schema you'll need to change it directly in the table properties file in the S3 config
bucket, and then reinitialise the table. An alternative is to delete the table and create a new table with the same
name.

### Rename/delete a table

You can rename or delete a table using the following commands:

```bash
./scripts/utility/renameTable.sh <instance-id> <old-table-name> <new-table-name>
./scripts/utility/deleteTable.sh <instance-id> <table-name>
```

You can also pass `--force` as an additional argument to deleteTable.sh to skip the prompt to confirm you wish to delete
all the data. This will permanently delete all data held in the table, as well as metadata.

### Take a table online/offline

You can take a table offline or put it online with the following commands:

```bash
./scripts/utility/takeTableOffline.sh <instance-id> <table-name>
./scripts/utility/putTableOnline.sh <instance-id> <table-name>
```

These scripts will set the table property `sleeper.table.online`, and update an index of table status to match.

You are still able to ingest files to offline tables, and perform queries against them. Here are some operations that
will not run for offline tables:

- Compaction job creation
- Partition splitting
- State store snapshot creation/deletion
- State store transaction deletion
- Garbage collection, unless the instance property `sleeper.run.gc.offline` is set to `true`
- Table metrics computation, unless the instance property `sleeper.run.table.metrics.offline` is set to `true`
