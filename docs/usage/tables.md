Tables
======

A Sleeper instance contains one or more tables. Each table has four important properties: a name, a schema for storing
data for that table, a state store for storing metadata about the table, and a flag to denote whether the table is
online or not. See the [design documentation](../design.md#Tables) for more information about online tables.

All resources for the instance, such as the S3 bucket used for storing data in a table, ECS clusters and lambda
functions are shared across all the tables.

## The metadata store

Each table has metadata associated to it. This metadata is stored in a state store and consists of information about
files that are in the system, and the partitions. See the [design documentation](../design.md#State_store) for more
information.

## Add/edit a table

Scripts can be used to add, rename and delete tables in a Sleeper instance. If using the scripts, creating a new table
will consist of the following steps:

1. Use the `estimateSplitPoints.sh` script to estimate split points from your data.
2. Use the `addTable.sh` script to create the table.
3. Use the `reinitialiseTable.sh` script with the split points from the first step.
4. Use the `sendToIngestBatcher.sh` script to send your data to the ingest batcher to be added to the table. See
   the [ingest documentation](ingest.md#ingest-batcher) for how to use this.

All of these scripts will rely on a schema for your table, which should be created first.
See [creating a schema](schema.md) for how to set up a schema for your table.

We also have scripts to rename and delete a table.

### Pre-split partitions

Before you create a Sleeper table, it is worthwhile to pre-split partitions for the table. One way to do this is by
taking a sample of your data to generate a split points file:

```bash
./scripts/utility/estimateSplitPoints.sh <schema-file> <num-partitions> <sketch-size> <output-split-points-file> <parquet-paths-as-separate-args>
```

The schema file should be the `schema.json` file you created for your table. You can calculate the number of partitions
by dividing the total number of rows you expect for your table by the average number of rows you want per partition.
The sketch size controls the size and accuracy of the data sketches used to estimate the split points. It should be a
power of 2 greater than 2 and less than 65536. See the Apache DataSketches documentation for more information:

https://datasketches.apache.org/docs/Quantiles/ClassicQuantilesSketch.html

You can apply the resulting split points when adding a table by setting an absolute path to the output file in the
table property `sleeper.table.splits.file`. If you haven't added any data to the table yet, you can apply the split
points by reinitialising the table.

### Add table

The `addTable.sh` script will create a new table with properties defined in `templates/tableproperties.template`, and a
schema defined in `templates/schema.template`. Currently any changes must be done in those templates or in the admin
client. We will add support for declarative deployment in the future.

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
