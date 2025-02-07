Tables
======

A Sleeper instance contains one or more tables. Each table has four important
properties: a name, a schema for storing data for that table, a state store for 
storing metadata about the table, and a flag to denote whether the table is 
online or not (see [here](../design.md#Tables) for more information about 
online tables). All resources for the instance, such as the S3 bucket used for 
storing data in a table, ECS clusters and lambda functions are shared across 
all the tables.

## The metadata store
Each table has metadata associated to it. This metadata is stored in a
StateStore and consists of information about files that are in the 
system, and the partitions.SThe storage of is in `S3StateStore`. 

The `S3StateStore` stores the metadata in Parquet files within the S3 bucket
used to store the table's data. When an update happens, a new file is
written containing the updated version of the metadata. To ensure that
two processes cannot both update the metadata at the same time leading
to a conflict, DynamoDB is used as a lightweight consistency layer.
Specifically an item in Dynamo is used to record the current version of
the metadata. When a client attempts to write a new version of the
metadata, it performs a conditional update to that item (the condition
is that the value is still the value the client based its update on).
If the update fails it means that another client updated it first, and
in this case the update is retried. As all the metadata is rewritten
on each update, there is no limit to the number of items that can be
read in a compaction job.

## Add/edit a table

Scripts can be used to add, rename and delete tables in a Sleeper instance.

The `addTable.sh` script will create a new table with properties defined in `templates/tableproperties.template`, and a
schema defined in `templates/schema.template`. Currently any changes must be done in those templates or in the admin
client. We will add support for declarative deployment in the future.

See [creating a schema](schema.md) for how to set up a schema for your table.

```bash
cd scripts
editor templates/tableproperties.template
editor templates/schema.template
./utility/addTable.sh <instance-id> <table-name>
./utility/renameTable.sh <instance-id> <old-table-name> <new-table-name>
./utility/deleteTable.sh <instance-id> <table-name>
```

You can also pass `--force` as an additional argument to deleteTable.sh to skip the prompt to confirm you wish to delete
all the data. This will permanently delete all data held in the table, as well as metadata.

## Reinitialise a table

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
