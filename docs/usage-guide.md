Interacting with an instance of Sleeper
=======================================

This contains information on the available methods to interact with a deployed instance of Sleeper.

This will assume you have a deployed instance of Sleeper, and you've installed the CLI as described in
the [getting started guide](getting-started.md). See the [deployment guide](deployment-guide.md) for more information on
deploying an instance.

If you just want to test locally, see the documentation on [deploying to localstack](deployment/deploy-to-localstack.md).
This has very limited functionality compared to a deployed instance.

## Build

It's currently necessary to build the system before any of the clients will work. In the future we may publish
pre-built artefacts that will make this unnecessary.

We will build the system in a container invoked with `sleeper builder`. Run the following commands:

```bash
sleeper builder # Create a Docker container with a workspace mounted in from the host directory ~/.sleeper/builder
git clone https://github.com/gchq/sleeper.git --branch main # Get the latest release version of Sleeper
cd sleeper # Navigate into the Git repository
./scripts/build/build.sh
```

This will take 20-40 minutes. This script creates the necessary artefacts and prepares the workspace to run the other
scripts. This build will be persisted to other invocations of `sleeper builder`, and you can run any of Sleeper's
scripts and tools in the command line this gives you.

## Configuration

Details of all Sleeper configuration properties are available here: [Properties](usage/property-master.md). These can be
edited in the administration client detailed below, or set during deployment.

## Operations on data

Data in Sleeper is held in a table. You can always add or remove Sleeper tables from an instance. See
the [tables documentation](usage/tables.md) for how to define and edit a table.

### Ingest

Data is ingested in large, sorted files which are then added to a Sleeper table. There are a number of options available
for creating these files and adding data to the system. See the [ingest documentation](usage/ingest.md) for details.

### Retrieving data

See the [data retrieval documentation](usage/data-retrieval.md) for ways to query a Sleeper table.

### Export

In the future it will be possible to export Sleeper table data in bulk. See the [data export documentation](usage/export.md).

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

## Java API

The clients module can be used as a dependency for an application to interact with Sleeper. This is not currently
published but is built with Maven. We have a class `SleeperClient` that can be used as an entrypoint for direct access
to an instance of Sleeper. This requires permissions to interact with the underlying AWS resources. We have an open
issue to introduce a REST API that may simplify this in the future (https://github.com/gchq/sleeper/issues/1786).

## Python API

See the [Python API documentation](usage/python-api.md) for details of the Python client library for Sleeper.

## Integrations

Experimental integrations are available to interact with Sleeper
via [Athena](usage/data-retrieval.md#use-athena-to-perform-sql-analytics-and-queries) and [Trino](usage/trino.md).
