Ingesting data
==============

## Introduction

Data in Sleeper tables is stored partitioned by the key and sorted within those partitions. Therefore when
Sleeper is given some data to ingest it must partition and sort it. This data must then be written to Parquet files
(one per leaf partition) and then the state store must be updated so that it is aware that the new data is in the
table.

To write data to Sleeper, you can either create a custom ingest process by interacting directly with the Sleeper code,
or:

1. Write your data to Parquet files in S3, in the data bucket or the configured ingest source bucket
2. Send a message to an SQS queue for an ingest system, as a job pointing to those files
3. Poll the ingest job tracker, and the ingest batcher store if you use the batcher, until the ingest is complete

The ingest system will sort your data and write it to one or more Parquet files in the Sleeper table.

Note that all ingest into Sleeper is done in batches - there is currently no option to ingest the data in a way
that makes it immediately available to queries. There is a trade-off between the latency of data being visible and
the cost, with lower latency generally costing more.

## Getting started

You can get started by using the script `sendToIngestBatcher.sh` to send your files to the SQS queue for the ingest
batcher, and `ingestBatcherReport.sh` and `ingestJobStatusReport.sh` to follow progress.

### Prepare files

Your files will need to be in the correct S3 bucket. To write your files to the data bucket deployed as part of Sleeper,
you can find the bucket name in the instance property `sleeper.data.bucket`,
documented [here](properties/instance/cdk/common.md). To use your own S3 bucket in the same AWS account, you can give
Sleeper access to it by setting its name in the instance property `sleeper.ingest.source.bucket`,
documented [here](properties/instance/user/ingest.md). You can use
the [administration client](../usage-guide.md#sleeper-administration-client) to find and set the values of these
properties.

Note that bulk import requires a minimum number of partitions, and by default a table starts with just one. The minimum
is set in the table property `sleeper.table.bulk.import.min.leaf.partitions`,
documented [here](properties/table/bulk_import.md). If too few partitions are present, then when a bulk import job is
submitted the partitions will be split automatically, based on the data in the bulk import job. This will assume that
the job's data is a representative sample for the table. You can take control over this by pre-splitting the table as
described [here](../usage/tables.md#pre-split-partitions).

The ingest batcher uses bulk import by default.

### Ingest with scripts

Here's an example of how to use the scripts to ingest with the batcher:

```bash
./scripts/utility/sendToIngestBatcher.sh <instance-id> <table-name> bucket-name/path/to/file.parquet bucket-name/path/to/folder
./scripts/utility/ingestBatcherReport.sh <instance-id> standard -a
./scripts/utility/ingestJobStatusReport.sh <instance-id> <table-name> standard -a
```

The batcher will wait until enough data is present in the files to create an ingest or bulk import job. You can
configure the batcher in table properties documented [here](properties/table/ingest_batcher.md), using `adminClient.sh`.

The files should show up in the ingest batcher report fairly quickly. Once the batcher has created jobs you can see
job IDs against the files in the batcher report. Once the jobs are picked up by ingest or bulk import, you can follow
them in the ingest job status report.

### Ingest with SQS

You can also submit files or jobs directly to an SQS queue, either for the ingest batcher or to ingest or bulk import.
You can find the queue URLs in instance properties documented under [ingest](properties/instance/cdk/ingest.md)
and [bulk import](properties/instance/cdk/bulk_import.md), using `adminClient.sh`. Which queues are available will
depend on which optional stacks are deployed.

In Java or Python, the class `SleeperClient` includes methods to read these properties and submit to a queue for you.

Here's an example of an SQS message for an ingest or bulk import job:

```json
{
  "id": "a-unique-id",
  "tableName": "my-table",
  "files": [
    "bucket-name/path/to/file.parquet",
    "bucket-name/path/to/directory"
  ]
}
```

Files are submitted to the ingest batcher with the same format, but without the `id` field.

## Ingest systems

You can find details of the available ingest systems in the following documents:

- [Standard Ingest](standard-ingest.md)
- [Bulk Import](bulk-import.md)
- [Ingest Batcher](ingest-batcher.md)

### Choosing an ingest system

There are two types of system for ingesting data: standard ingest and bulk import. The former refers to a process that
reads data and partitions and sorts it locally before writing it to files in S3. Bulk import means using
[Apache Spark](https://spark.apache.org/) to run a MapReduce-like job to partition and sort a batch of data as a
distributed process.

For ingesting any significant volumes of data, the bulk import process is preferred because it is faster and results in
less compaction work later. The standard ingest process is mainly used for testing purposes.

The standard ingest process can be called from Java on any `Iterable` of `Row`s, with the class `IngestFactory`.
There is also an `IngestStack` which deploys an ECS cluster, and creates ECS tasks to run ingest when you send a message
to an ingest queue in SQS.

The bulk import approach can be customised to run in your own Spark cluster, or Sleeper can run it for you via an ingest
queue in SQS. There are multiple stacks that can be deployed for this approach. `EmrServerlessBulkImportStack` runs the
Spark job on EMR Serverless. `EmrBulkImportStack` creates an EMR cluster on demand to run the Spark job. The cluster is
only used for that bulk import job. `PersistentEmrBulkImportStack` creates an EMR cluster that is permanently running.
By default it scales up and down so that if there are no bulk import jobs to run then minimal resources will be used.
`EksBulkImportStack` is an experimental option to run Spark on an EKS cluster.

If you have occasional bulk import jobs, or you just want to get started, then we recommend the serverless EMR approach.
If you will have a lot of jobs running fairly constantly, then the persistent EMR approach is recommended.

An `IngestBatcherStack` is also available to automatically group smaller files into jobs of a configurable size. These
jobs will be submitted to either standard ingest or bulk import, based on the configuration of the Sleeper table.

By default, a new Sleeper instance includes `EmrServerlessBulkImportStack`, `IngestStack` and `IngestBatcherStack`.

## What ingest rate does Sleeper support?

In theory, an arbitrary number of ingest jobs can run simultaneously. If the limits on your AWS account allowed
it, you could have 100 EMR clusters each running a job to import 10 billion rows. Each job will be writing
files to S3, and when it is finished the state store will be updated. All of these operations are independent.
Therefore the only limit is the capacity of the S3 bucket to receive data and the capacity of the state store to receive
updates. Thus if the 100 bulk import jobs complete at roughly the same time, the number of rows in the table would
increase by 1 trillion very quickly.

However, in order for a query to return results quickly, there needs to be a modest number of files in each
partition. If there are around 10 files in a partition, then queries will be quick. In the above example,
100 files would be added to each partition. A query for a key that ran immediately after those 100 jobs
finished would have to open all 100 files, and this would mean the query would be slow.
The conpaction process will run multiple compaction jobs to compact those 100 files together into a
smaller number of files. Once this is done, queries will be quick.

This example shows that ingest is a balancing act between adding data quickly and maintaining query performance.
If too many import jobs finish in a short period then query performance will suffer. A small number of large
import jobs is better than a large number of small jobs.

## Job tracker

If you submit your files to an ingest queue as a job, the progress is tracked in DynamoDB. This updates each time your
job progresses to the next step of the process.

In Java you can query this with an `IngestJobTracker` object, created by `IngestJobTrackerFactory`. You can set an ID
for your job in the message on the ingest queue, and then call `IngestJobTracker.getJob` to check its status. This API
is not currently stable. At time of writing, you can check the status of the job by calling `getFurthestRunStatusType`
on the status object.

We may add a REST API to serve this purpose in the future. You can also use the following script to check the status
of jobs manually:

```bash
./scripts/utility/ingestJobStatusReport.sh <instance-id> <table-name> <report-type-standard-or-json> <optional-query-type> <optional-query-parameters>
```

For example:

```bash
./scripts/utility/ingestJobStatusReport.sh <instance-id> <table-name> # Prompt for report type
./scripts/utility/ingestJobStatusReport.sh <instance-id> <table-name> standard -a # All jobs
./scripts/utility/ingestJobStatusReport.sh <instance-id> <table-name> standard -u # Unfinished jobs
./scripts/utility/ingestJobStatusReport.sh <instance-id> <table-name> standard -n # Rejected jobs
./scripts/utility/ingestJobStatusReport.sh <instance-id> <table-name> standard -r 20250523090000,20250523100000 # Date range in format yyyyMMddhhmmss
./scripts/utility/ingestJobStatusReport.sh <instance-id> <table-name> standard -d <job-id> # Job details
```

Note that if a run of the job has finished, it may still be uncommitted. This means the data has been sorted and written
to files in S3, but it is not yet in the Sleeper table. The result is then committed to the state store to add the files
to the Sleeper table.

## Ingest batcher store

If you use the ingest batcher, files are submitted without any attachment to a job, and the batcher groups them into
jobs later on. The batcher store tracks these files in DynamoDB, including any job they have been added to. You can then
look up jobs in the job tracker by the IDs held against your files.

In Java you can query this with an `IngestBatcherStore` object, created by `IngestBatcherStoreFactory`. When your files
are added to a job you can find a job ID on the `IngestBatcherTrackedFile` object.

We may add a REST API to serve this purpose in the future. You can also use the following script to check the status
of files manually:

```bash
./scripts/utility/ingestBatcherReport.sh <instance-id> <report-type-standard-or-json> <optional-query-type>
```

For example:

```bash
./scripts/utility/ingestBatcherReport.sh <instance-id> # Prompt for report type
./scripts/utility/ingestBatcherReport.sh <instance-id> standard -a # All files
./scripts/utility/ingestBatcherReport.sh <instance-id> standard -p # Pending files (not yet in a job)
```
