Interacting with an instance of Sleeper
=======================================

This contains information on the available methods to interact with a deployed instance of Sleeper.

This will assume you have a deployed instance of Sleeper, and you've built the system locally in a builder container.
See the [getting started guide](getting-started.md) for how to set that up. Also see
the [deployment guide](deployment-guide.md) for more information on deploying an instance.

If you just want to test locally, see the documentation on [deploying to localstack](usage/deploy-to-localstack.md).
This has very limited functionality compared to a deployed instance.

## Tables

Data in Sleeper is held in a table. You can always add or remove Sleeper tables from an instance. See
the [tables documentation](usage/tables.md) for how to define and edit a table.

## Clients and scripts

There are clients and scripts in the `scripts/deploy` and `scripts/utility` directories that can be used to work with an
existing instance.

Also see the [tables documentation](usage/tables.md#addedit-a-table) for scripts to add/edit Sleeper tables.

### Sleeper Administration Client

We have provided a command line client that will enable you to:

1) List Sleeper instance properties
2) List Sleeper table names
3) List Sleeper table properties
4) Change an instance/table property
5) Get status reports (also see [checking the status of the system](usage/status.md))

This client will prompt you for things like your instance ID as mentioned above and/or the name of the table you want to
look at. To adjust property values it will open a text editor for a temporary file.

You can run this client with the following command:

```bash
./scripts/utility/adminClient.sh ${INSTANCE_ID}
```

### Compact all files

If you want to fully compact all files in leaf partitions, but the compaction strategy is not compacting files in a
partition, you can run the following script to force compactions to be created for files in leaf partitions that were
skipped by the compaction strategy:

```bash
./scripts/utility/compactAllFiles.sh ${INSTANCE_ID} <table-name-1> <table-name-2> ...
```
