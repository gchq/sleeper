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
3. Poll the ingest job tracker until the ingest is complete

The ingest system will sort your data and write it to one or more Parquet files in the Sleeper table.

The data bucket name can be found in the instance property `sleeper.data.bucket`,
documented [here](properties/instance/cdk/common.md). You can set your own source bucket name in the instance
property `sleeper.ingest.source.bucket`, documented [here](properties/instance/user/ingest.md).

You can choose which ingest system to use for this by which SQS queue you send your message to. You can find the queue
URLs in instance properties like `sleeper.ingest.job.queue.url`, documented
under [ingest](properties/instance/cdk/ingest.md) and [bulk import](properties/instance/cdk/bulk_import.md).
Which queues are available will depend on which optional stacks are deployed.

Here's an example of an SQS message for an ingest or bulk import job:

```json
{
  "id": "a_unique_id",
  "tableName": "myTable",
  "files": [
    "databucket/file.parquet",
    "databucket/directory/path"
  ]
}
```

Note that all ingest into Sleeper is done in batches - there is currently no option to ingest the data in a way
that makes it immediately available to queries. There is a trade-off between the latency of data being visible and
the cost, with lower latency generally costing more.

You can also submit your files to the ingest batcher, which will group larger batches of data into ingest or bulk import
jobs. In this case you won't be able to use the ingest job tracker directly, but you can find which jobs are created
from which files by querying the ingest batcher store. The SQS queue URL can be find in the instance
property `sleeper.ingest.batcher.submit.queue.url`. The ingest batcher can be configured in table properties
documented [here](properties/table/ingest_batcher.md). Here's an example of an SQS message to submit to the ingest
batcher:

```json
{
  "tableName": "myTable",
  "files": [
    "databucket/file.parquet",
    "databucket/directory/path"
  ]
}
```

## Choosing an ingest system

There are two types of system for ingesting data: standard ingest and bulk import. The former refers to a process that
reads data and partitions and sorts it locally before writing it to files in S3. Bulk import means using
[Apache Spark](https://spark.apache.org/) to run a MapReduce-like job to partition and sort a batch of data as a
distributed process.

For ingesting any significant volumes of data, the bulk import process is preferred because it is faster and results in
less compaction work later. The standard ingest process is mainly used for testing purposes.

The standard ingest process can be called from Java on any `Iterable` of `Record`s, with the class `IngestFactory`.
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

You can find details of the available ingest systems in the following documents:

- [Standard Ingest](standard-ingest.md)
- [Bulk Import](bulk-import.md)
- [Ingest Batcher](ingest-batcher.md)

## What ingest rate does Sleeper support?

In theory, an arbitrary number of ingest jobs can run simultaneously. If the limits on your AWS account allowed
it, you could have 100 EMR clusters each running a job to import 10 billion records. Each job will be writing
files to S3, and when it is finished the state store will be updated. All of these operations are independent.
Therefore the only limit is the capacity of the S3 bucket to receive data and the capacity of the state store to receive
updates. Thus if the 100 bulk import jobs complete at roughly the same time, the number of records in the table would
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
job progresses to the next step of the process. In Java you can query this with an `IngestJobTracker` object, created
by `IngestJobTrackerFactory`. You can set an ID for your job in the message on the ingest queue, and then call
`IngestJobTracker.getJob` to check its status. This API is not currently stable. We may add a REST API to serve
this purpose in the future. At time of writing, you can check the status of the job by calling
`getFurthestRunStatusType` on the status object.

Note that if a run of the job has finished, it may still be uncommitted. This means the data has been sorted and written
to files in S3, but it is not yet in the Sleeper table. The result is then committed to the state store to add the files
to the Sleeper table.
