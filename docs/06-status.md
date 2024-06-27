Checking the status of the system
=================================

There are two main ways to check the status of the system: use the metrics in the CloudWatch console,
or use the provided scripts to get a textual summary of various aspects of the system.

These instructions will assume you start in the project root directory and you're using a development environment
(see [the dev guide](11-dev-guide.md) for how to set that up).

## CloudWatch metrics

To view the CloudWatch metrics the `DashboardStack` must be enabled. This can be achieved by ensuring
the `sleeper.optional.stacks` property contains the option `DashboardStack` when the instance is deployed.
To view the metrics, go to the CloudWatch service in the console, click on the "All metrics" option on the
left-hand side, select "Sleeper" under "Custom namespaces" then select "instanceId, tableName" and then
search for your instance id in the search box. Select the metrics you are interested in and then some
graphs will appear.

## Status reports

We provide utility classes for various administrative tasks, such as seeing the
number of records in a table, and for pausing, restarting and changing properties
in the system.

You should make sure that you run any of the commands
under the same AWS profile that you created Sleeper with.

You can change profile using:

```bash
export AWS_PROFILE=<INSERT-PROFILE-NAME>
export AWS_REGION=eu-west-2
```

The following notes all assume that you have stored your instance id
in the INSTANCE_ID environment variable:

```bash
export INSTANCE_ID=mySleeper
```

All status reports can be run using the scripts in the `utility` directory, [here](../scripts/utility). They require
your Sleeper instance id. Some of the reports also require a table name. Some offer a standard option and a verbose
option.

The available reports are as follows, with the corresponding commands to run them:

| Report Name                | Description                                       | Command                                                                                                                                                             | Defaults                                                           |
|----------------------------|---------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| Full Status Report         | Prints all of the below reports in one go         | ```./scripts/utility/fullStatusReport.sh ${INSTANCE_ID} ${TABLE_NAME} ${OPTIONAL_VERBOSE_OUTPUT}```                                                        | VERBOSE = false                                                    |
| Dead Letters Status Report | Prints out any messages on the dead letter queues | ```./scripts/utility/deadLettersStatusReport.sh ${INSTANCE_ID}```                                                                                          |                                                                    |
| ECS Tasks Status Report    | Lists the compaction tasks                        | ```./scripts/utility/ecsTasksStatusReport.sh ${INSTANCE_ID}```                                                                                             |                                                                    |
| Files Status Report        | Lists all the files managed by the state store    | ```./scripts/utility/filesStatusReport.sh ${INSTANCE_ID} ${TABLE_NAME} ${OPTIONAL_MAXREADY_GC_FILES} ${OPTIONAL_VERBOSE_OUTPUT} ${OPTIONAL_REPORT_TYPE}``` | MAXREADY_GC_FILES = 1000, VERBOSE = false , REPORT_TYPE = standard |
| Jobs Status Report         | Prints the number of messages on job queues       | ```./scripts/utility/jobsStatusReport.sh ${INSTANCE_ID}```                                                                                                 |                                                                    |
| Partitions Status Report   | Summarises the partitions within the system       | ```./scripts/utility/partitionsStatusReport.sh ${INSTANCE_ID} ${TABLE_NAME}```                                                                             |                                                                    |

## Retrying messages on DLQs

In addition to checking the dead letter queues, you can also send messages on them back to the original queue so that
the processing is retried.

The queue must be one of:

* `query`
* `compaction`
* `ingest`

Here's an example:

```bash
# Retry up to 1000 messages on the ingest dead letter queue
java -cp jars/clients-*-utility.jar \
  sleeper.clients.status.report.RetryMessages ${INSTANCE_ID} ingest 1000
```

## Pausing and Restarting the System

If there is no ingest in progress, and all compactions have completed, then Sleeper will go to sleep, i.e. the only
significant ongoing charges are for data storage. However, there are several lambda functions that are scheduled to
run periodically using EventBridge rules. These lambda functions look for work to do, such as compactions to run.
The execution of these should have very small cost, but it is best practice to pause the system,
i.e. turn these rules off, if you will not be using it for a while. Note that the system can still be queried when
it is paused.

```bash
# Pause the System
./scripts/utility/pauseSystem.sh ${INSTANCE_ID}

# Restart the System
./scripts/utility/restartSystem.sh ${INSTANCE_ID}
```

## Reinitialise a Table

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

If you want to change the table schema make sure you change the schema in the table properties file in the S3
config bucket.

## Sleeper Administration Client

We have provided a command line client that will enable you to:

1) List Sleeper instance properties
2) List Sleeper table names
3) List Sleeper table properties
4) Change an instance/table property

This client will prompt you for things like your instance id as mentioned above and/or
the name of the table you want to look at, the name of the property you want to update and its new value.

To run this client you can run the following command:

```bash
./scripts/utility/adminClient.sh ${INSTANCE_ID}
```

## Compact all files

If you want to fully compact all files in leaf partitions, but the compaction strategy is not compacting files in a
partition, you can run the following script to force compactions to be created for files in leaf partitions that were
skipped by the compaction strategy:

```bash
./scripts/utility/compactAllFiles.sh ${NSTANCE_ID}
```