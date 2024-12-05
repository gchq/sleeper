Checking the status of the system
=================================

There are two main ways to check the status of the system: use the metrics in the CloudWatch console,
or use the provided scripts to get a textual summary of various aspects of the system.

These instructions will assume you start in the project root directory and Sleeper has been built
(see [the developer guide](11-dev-guide.md) for how to set that up).

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

The available reports are as follows. They can be accessed through the admin client
with `./scripts/utility/adminClient.sh ${INSTANCE_ID}`, or with the commands below:

| Report Name                   | Description                                       | Command                                                                                                                                                             | Defaults                                                           |
|-------------------------------|---------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| Full Status Report            | Prints all of the below reports in one go         | ```./scripts/utility/fullStatusReport.sh ${INSTANCE_ID} ${TABLE_NAME} ${OPTIONAL_VERBOSE_OUTPUT}```                                                        | VERBOSE = false                                                    |
| Dead Letters Status Report    | Prints out any messages on the dead letter queues | ```./scripts/utility/deadLettersStatusReport.sh ${INSTANCE_ID}```                                                                                          |                                                                    |
| Ingest Task Status Report     | Lists the ingest tasks                            | ```./scripts/utility/ingestTaskStatusReport.sh ${INSTANCE_ID} ${OPTIONAL_REPORT_TYPE} ${OPTIONAL_QUERY_TYPE} ${OPTIONAL_QUERY_PARAMETERS}``` | REPORT_TYPE = standard, QUERY_TYPE = prompt |
| Ingest Job Status Report      | Lists the ingest and bulk import jobs and the number of messages on the queues | ```./scripts/utility/ingestJobStatusReport.sh ${INSTANCE_ID} ${TABLE_NAME} ${OPTIONAL_REPORT_TYPE} ${OPTIONAL_QUERY_TYPE} ${OPTIONAL_QUERY_PARAMETERS}``` | REPORT_TYPE = standard, QUERY_TYPE = prompt |
| Compaction Task Status Report | Lists the compaction tasks                        | ```./scripts/utility/compactionTaskStatusReport.sh ${INSTANCE_ID} ${OPTIONAL_REPORT_TYPE} ${OPTIONAL_QUERY_TYPE} ${OPTIONAL_QUERY_PARAMETERS}``` | REPORT_TYPE = standard, QUERY_TYPE = prompt |
| Compaction Job Status Report  | Lists the compaction jobs and the number of messages on the queue | ```./scripts/utility/compactionJobStatusReport.sh ${INSTANCE_ID} ${TABLE_NAME} ${OPTIONAL_REPORT_TYPE} ${OPTIONAL_QUERY_TYPE} ${OPTIONAL_QUERY_PARAMETERS}``` | REPORT_TYPE = standard, QUERY_TYPE = prompt |
| Files Status Report           | Lists all the files managed by the state store    | ```./scripts/utility/filesStatusReport.sh ${INSTANCE_ID} ${TABLE_NAME} ${OPTIONAL_MAXREADY_GC_FILES} ${OPTIONAL_VERBOSE_OUTPUT} ${OPTIONAL_REPORT_TYPE}``` | MAXREADY_GC_FILES = 1000, VERBOSE = false , REPORT_TYPE = standard |
| Partitions Status Report      | Summarises the partitions within the system       | ```./scripts/utility/partitionsStatusReport.sh ${INSTANCE_ID} ${TABLE_NAME}```                                                                             |                                                                    |

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
java -cp scripts/jars/clients-*-utility.jar \
  sleeper.clients.status.report.RetryMessages ${INSTANCE_ID} ingest 1000
```
