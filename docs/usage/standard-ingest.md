Standard Ingest
===============

Sleeper's standard ingest process is based around the class `sleeper.ingest.runner.IngestFactory`. This contains a
method that can be called on any `Iterable` of `Row`s. This process reads a large batch of rows from the
`Iterable` and prepares it for writing into the Sleeper table. This involves: reading a batch of rows
into memory, sorting them by key and sort fields, and then writing that batch to a local file. This is
repeated some number of times. At this point the data is only written locally and it is not inside
the Sleeper table. These local files are read into a sorted iterable and the data is written to files in
the S3 bucket for the table. One file is written for each of the leaf partitions. Once this is done,
these new files are added to the state store. At this point the data is available for query.

In this process there is a trade-off between the cost and the delay before data is available for query.
Each batch of rows has to be written to files in S3, one per leaf partition. Suppose there were
10,000 rows in the batch (i.e. the close() method was called on the ingest method after 10,000
rows had been added), and suppose the table was split into 100 leaf partitions. Then each write to
S3 would be of a file containing 100 rows. To write these 10,000 rows would require 100 S3 PUTs
and 100 DynamoDB PUTs. Writing 10,000 rows like this would have almost negligible cost. However,
writing 100 million rows with batches of 10,000 rows, would cause 1 million S3 PUTs which costs
around $5 and $1.25 in DynamoDB PUT costs. If the batch was 10 million rows, then the cost of the
required S3 PUTs would be $0.005 and $0.00125 for DynamoDB costs. Therefore in general larger batches
are better, but there is a longer delay before the data is available for query.

To make it easy for users to ingest data from any language, and to deploy ingest jobs in a scalable way,
there is an ingest stack of cloud components. This requires the user to write data to Parquet files,
with columns matching the fields in your schema (note that the fields in the schema of the Parquet file
all need to be non-optional).

Note that the descriptions below describe how data in Parquet files can be ingested by sending ingest job
definitions in JSON form to SQS queues. In practice it may be easier to use the [Python API](../usage/python-api.md).

When you have the data you want to ingest stored in Parquet files, a message should be sent
to Sleeper's ingest queue telling it that the data should be ingested. This message should have the following form:

```JSON
{
  "id": "a_unique_id",
  "tableName": "myTable",
  "files": [
    "databucket/file1.parquet",
    "databucket/file2.parquet"
  ]
}

```

Here the items listed under `files` can be either files or directories. If they are directories, then Sleeper
will recursively look for files ending in `.parquet` within them.

Files to be ingested must be accessible to the ECS tasks that run the jobs. See
the [ingest guide](ingest.md#prepare-files) for how to prepare your files for access.

It is up to you to spread the data you want to ingest over an appropriate number of jobs. As a general rule,
aim for at least 10s of millions of rows per job.

The id field will be used in logging so that users can see the progress of particular ingest jobs by viewing the
logs. The URL of the SQS queue that the message should be sent to can be found from the `sleeper.ingest.job.queue.url`
property. This will be populated in the config object in the `sleeper-<instance-id>-config` S3 bucket. It can also
be found using the [administration client](../usage-guide.md#sleeper-administration-client).

You will need to ensure that the role with the ARN given by the `IngestContainerRoleARN` property has read access
to the files you wish to ingest. This ARN is exported as a named export from CloudFormation with name
`<sleeper-id>-IngestContainerRoleARN` to help stacks that depend on Sleeper automatically grant read access to their
data to Sleeper's ingest role. A simple way to do this is to use the `sleeper.ingest.source.bucket` instance property to
set the name of the bucket that the files are in. If this property is populated when the Sleeper instance is deployed
then the ingest roles will be granted read access to it. Bulk import systems will also be granted read access to it.

Once the message has been sent to the SQS, a lambda will notice that there are messages on the queue and then start
a task on the ingest ECS cluster (this cluster is called `sleeper-<instance-id>-ingest-cluster`). This task will
then ingest the data. This process is asynchronous, i.e. it may be several minutes before the data has been ingested.
