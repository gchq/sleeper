Checking the status of the system
=================================

We provide utility classes for various administrative tasks, such as seeing the
number of records in a table, and for pausing, restarting and changing properties
in the system.

Start by building the code (note that this build script copies
the jars into the `scripts/jars` directory so that the scripts
can use them):
```bash
./scripts/build/build.sh
```

You should also make sure that you run any of the commands 
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

## Status Reports

All status reports can be run using the scripts in the `scripts/utility`
directory. They require your Sleeper instance id. Some of the reports also
require a table name. Some offer a standard option and a verbose option.

The available reports are as follows, run the commands in the top 
level project directory to produce them:

| Report Name                | Description                                       | Command                                                                                                                                                    | Defaults                                                           |
|----------------------------|---------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| Full Status Report         | Prints all of the below reports in one go         | ```./scripts/utility/fullStatusReport.sh ${INSTANCE_ID} ${TABLE_NAME} ${OPTIONAL_VERBOSE_OUTPUT}```                                                        | VERBOSE = false                                                    |
| Dead Letters Status Report | Prints out any messages on the dead letter queues | ```./scripts/utility/deadLettersStatusReport.sh ${INSTANCE_ID}```                                                                                          |                                                                    |
| ECS Tasks Status Report    | Lists the compaction / splitting tasks            | ```./scripts/utility/ecsTasksStatusReport.sh ${INSTANCE_ID}```                                                                                             |                                                                    |
| Files Status Report        | Lists all the files managed by the state store    | ```./scripts/utility/filesStatusReport.sh ${INSTANCE_ID} ${TABLE_NAME} ${OPTIONAL_MAXREADY_GC_FILES} ${OPTIONAL_VERBOSE_OUTPUT} ${OPTIONAL_REPORT_TYPE}``` | MAXREADY_GC_FILES = 1000, VERBOSE = false , REPORT_TYPE = standard |
| Jobs Status Report         | Prints the number of messages on job queues       | ```./scripts/utility/jobsStatusReport.sh ${INSTANCE_ID}```                                                                                                 |                                                                    |
| Partitions Status Report   | Summarises the partitions within the system       | ```./scripts/utility/partitionsStatusReport.sh ${INSTANCE_ID} ${TABLE_NAME}```                                                                             |                                                                    |

## Retrying messages on DLQs

In addition to checking the dead letter queues, you can also send messages on them
back to the original queue so that the processing is retried.

The queue must be one of:
* `query`
* `compaction`
* `splittingcompaction`
* `ingest`

Here's an example:
```bash
# Retry up to 1000 messages on the ingest dead letter queue
cd java
java -cp clients/target/clients-*-utility.jar \
sleeper.status.report.RetryMessages ${INSTANCE_ID} ingest 1000
```

## Pausing and Restarting the System
If there is no ingest in progress, and all compactions have completed, then Sleeper
will go to sleep, i.e. the only significant on-going charges are for data storage.
However there are several lambda functions that are scheduled to run periodically
using EventBridge rules. These lambda functions look for work to do, such as compactions
to run. The execution of these should have very small cost, but it is best practice
to pause the system, i.e. turn these rules off, if you will not be using it for a
while. Note that the system can still be queried when it is paused.

```bash
# Pause the System
./scripts/utility/pauseSystem.sh ${INSTANCE_ID}

# Restart the System
./scripts/utility/restartSystem.sh ${INSTANCE_ID}
```

## Reinitialise a Table
Reinitialising a table means deleting all its contents. This can sometimes be useful when you are experimenting
with Sleeper or if you created a table with the wrong schema.

You can reinitialise the table quickly by running the following script from the project root:

```bash
./scripts/utility/reinitialiseTable.sh <Instance id> <Table Name> <OPTIONAL_delete_partitions_true_or_false> <OPTIONAL_split_points_file_location> <optional_split_points_file_base64_encoded_true_or_false>
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

To run this client you can run the following command from the project root:

```bash
./scripts/utility/adminClient.sh ${INSTANCE_ID}
```