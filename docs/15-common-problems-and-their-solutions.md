Common problems and their solutions
===================================

These instructions will assume you start in the project root directory and you have the required dependencies
(see [Installing prerequisite software](11-dev-guide.md#install-prerequisite-software) for how to set that up).

## EOFException when using client classes

An exception of the following form might be due to a change in schema:

```bash
Caused by: java.io.EOFException
     at java.io.DataInputStream.readFully(DataInputStream.java:197)
     at java.io.DataInputStream.readUTF(DataInputStream.java:609)
     at java.io.DataInputStream.readUTF(DataInputStream.java:564)
Exception in thread "main" sleeper.core.statestore.StateStoreException: Exception querying DynamoDB
```

When cdk deploy is used to deploy a table, the state store is initialised. This initialisation writes the initial
partitions to the partitions part of the state store (if using the default DynamoDB state store then this will be a
table named `sleeper-<instance-id>-table-<table-name>-partitions`). The partitions are specific to the schema of the
table (specifically to the row keys specified). Therefore if you deploy a table with a certain schema, and then attempt
to change the schema by updating the table properties file, any interactions with the partitions table will not be able
to read the partitions.

If you want to reinitialise a table with a new schema, see the section below on reinitialising a table.

## Out of memory error from standard ingest tasks

If standard ingest tasks fail with an out of memory error ("Exception in thread main java.lang.OutOfMemoryError: Java
heap space") then this is likely due to the tasks not being able to store the specified number of records in memory.
Standard ingest works by reading a certain number of records (given by `sleeper.ingest.memory.max.batch.size`) into
memory. These are sorted and then written to a local file. This process is repeated a certain number of times until a
certain number of records in total (given by `sleeper.ingest.max.local.records`) have been written to local disk.
Sensible values for the parameters `sleeper.ingest.memory.max.batch.size` and `sleeper.ingest.max.local.records`
obviously depend on the data - the more fields the schema has and the bigger those fields are, the more space will be
used and the fewer records will fit into memory / on disk.

If you see an out of memory error, then try reducing `sleeper.ingest.memory.max.batch.size`. When reducing this
parameter it is a good idea to also reduce `sleeper.ingest.max.local.records`. To change these parameters, use the
administration client described in the [system status documentation](06-status.md).

## I need to reinitialise a table

There are two reasons you might want to reinitialise a table: you want to delete the contents of a table and start
with an empty table with the same schema or you may have got the schema wrong and want to start again with a new one.
This can be achieved by deleting the table and then creating it again, but the following steps may well be quicker:

You can reinitialise the table quickly by running the following command - note that reinitialising a table will delete
all data in the table. If you want to change the table schema make sure you change
the schema in the table properties before running this script:

```bash
./scripts/utility/reinitialiseTable.sh <instance-id> <table-name> <optional-delete-partitions-true-or-false> <optional-split-points-file-location> <optional-split-points-file-base64-encoded-true-or-false>
```

e.g.

```bash
./scripts/utility/reinitialiseTable.sh my-sleeper-config-bucket my-sleeper-table true /tmp/split-points.txt false
```

Alternatively you can use a more manual approach. This may be better if you want to significantly change the table
properties.

For a table with a Dynamo DB state store:

- Delete all the files `partition*/*` in the S3 bucket for the table (the bucket will be named
  `sleeper-<instance-id>-table-<table-name>`). This can be done either in the AWS console or using the CLI.
- Delete all the entries in the DynamoDB state store tables: `sleeper-<instance-id>-table-<table-name>-active-files`,
  `sleeper-<instance-id>-table-<table-name>-gc-files`, `sleeper-<instance-id>-table-<table-name>-partitions`.
- If you want to change the schema of the table, then edit the table properties file in the config bucket in S3.
- Reinitialise the state store:

```bash
java -cp jars/clients-*-utility.jar sleeper.statestore.InitialiseStateStore <instance-id> <table-name>
```

For a table with an S3 state store:

- Delete all the files `partition*/*` in the S3 bucket for the table (the bucket will be named
  `sleeper-<instance-id>-table-<table-name>`). This can be done either in the AWS console or using the CLI.
- Delete all the files `statestore/*` in the same S3 bucket for the table.
- Delete all the entries in the DynamoDB table: `sleeper-<instance-id>-table-<table-name>-revisions`,
- If you want to change the schema of the table, then edit the table properties file in the config bucket in S3.
- Reinitialise the state store:

```bash
java -cp jars/clients-*-utility.jar sleeper.statestore.InitialiseStateStore <instance-id> <table-name>
```

## Why is adding a table to an existing Sleeper instance so slow?

Each table stores its data in its own bucket, and has its own state store (by default the DynamoDB-based state store
is used which uses 3 DynamoDB tables). When a new Sleeper table is created, these resources all need to be created
and then the various components of Sleeper (lambda, ECS containers, etc) all need to have the permissions of their
roles updated so that they can interact with these resources. This is done using cdk and it takes several minutes.

## I created an instance, destroyed it and then recreating it failed

If you create an instance and destroy it then some remnants of the previous instance will still be present. Usually this
should be log groups containing logs of the previous instance.

The CDK deployment process can also be configured to not delete the buckets for the tables, or the bucket for the
results of queries. This is set in the `sleeper.retain.infra.after.destroy` instance property. It may also be because
the `cdk destroy` command partially failed due to there being some tasks running on ECS or EMR clusters. In this case
the cluster cannot be destroyed until the tasks are completed or terminated.

If there are some remnants present then attempting to deploy Sleeper again with the same instance id will fail as it
will complain that some resources it needs to create already exist.

If you want to recreate an instance with the same instance id as one that was previously deleted, then check
that all resources with a name containing that instance id have been deleted.
