Design
======

This section describes the design of Sleeper. It starts with an overview of the data structure that Sleeper is based
on, followed by a detailed description of the design.

Sleeper is based on the log-structured merge tree. This data structure is designed to support high rates of data
ingest, and to support quick retrieval of records by a key field. To achieve this, the records are stored in a small
number of files that are sorted by key. As records are continually arriving, it is necessary to merge old files with
new files in order to ensure that the number of files remains small. This is called a compaction. As Sleeper is a
large-scale system, data is also range-partitioned by key. Over time partitions can be split if they become larger
than a threshold.

![High level design diagram](diagrams/high-level-design.png)

## A Sleeper instance

An instance of Sleeper is identified by a string id that should be globally unique across AWS. A Sleeper
instance has a set of instance properties associated with it. Some of these properties are defined by the user
when the instance is created, others are defined by the CDK deployment process. These properties are stored
in a properties file, which is stored in a bucket in S3. The name of this S3 bucket is `sleeper-` followed by the
instance id followed by `-config`, e.g. `sleeper-mySleeperInstance-config`.

An instance of Sleeper can contain one or more tables. Each table contains records with fields matching a schema.
Each table has its own S3 bucket for storing data and a state store for storing metadata about that table.

The Sleeper instance also contains infrastructure to ingest data, compact data, garbage collect data, split
partitions, execute queries, and run Athena queries. Each of these are provided by a separate CDK stack. All of
these are optional, but in practice the compaction, garbage collection and partition splitting stacks are essential.

## Records

Records are the fundamental unit of data in Sleeper. A record is simply a map from a field name to a value, i.e.
`Map<String, Object>` where the object should be one of the supported types. For example, we might have a record
with 3 fields: `id -> "abc"`, `timestamp -> 1234567980`, `value -> "hello"`.

## Schema

A schema specifies the fields that will be found in records in a table. Each field has a name and a type. There are
three different classes of fields: row fields, sort fields, and value fields. The row fields are used to partition
and sort the data, i.e. all records with the same values of the row fields are within the same partition, and a
partition contains a range of values of the row fields. Within a partition, records are stored sorted by the row
fields and then the sort fields. The following types are supported for row and sort fields: int, long, string,
byte array. Value fields can be one of these primitive types but can also be of map or list type.

Sleeper is designed to allow quick retrieval of records where the key field is a given value, or where the key
field is in a certain range. Note that the row fields and sort fields are ordered, e.g. if there are two row key
fields id1 and id2, then records are stored sorted by id1 and, in the case of ties, by id2. This means that queries
for records where id1 = x and id2 = y will be quick, as will queries that just specify id1. But queries that just
specify id2 will not be quick as they will require a full scan of the table (although file-level statistics can
sometimes be used to avoid reading a lot of the data).

## Tables

All records in a table conform to a schema. The records in a table are stored in multiple files, with each file
belonging to a partition. These files are all in an S3 bucket that is exclusively used by this table.

Each table has a state store associated to it. This stores metadata about the table, namely the files that are in
the table and how the records in the table are partitioned.

Tables are deployed by the CDK table stack. This stack creates the infrastructure for each table. Each table
requires a bucket where its data will be stored, and a state store. When a table is first created, its state store
must be initialised.

To achieve this, the table stack obtains a list of the table properties files from the instance properties. For
each table properties file, it creates the data bucket for that table and creates the state store. The creation
of the state store is delegated to either the DynamoDB state store stack or the S3 state store stack, as appropriate.
A custom CDK resource is then used to call `sleeper.cdk.custom.SleeperTableLambda`. This lambda initialises the
state store and updates the table properties file which is stored in the instance's config bucket.

The name of the bucket containing a table's data is called `sleeper-<instance-id>-table-tablename`, e.g.
`sleeper-mySleeperInstance-table-table1`.

## Sorted files

Records in a table are stored in files. The records in each file are stored sorted by row key and sort keys. Currently
data is always stored in Parquet files (support for Rfiles will be added). The files used must support retrieving
individual rows by key (assuming the file is sorted by key) without needing to read the entire file. Parquet supports
this because it stores a footer than contains the minimum and maximum values of each column, by both row group and
page. This means that to retrieve a row where the key field takes a value only requires reading one page per column.
Pages are small (128KiB by default), so even if the file is many GiBs in size only a few hundred KiB need to be read
to retrieve an individual row.

## Partitions

Data within a table is split into partitions. Each partition contains records with keys from a certain range, e.g.
if the row key field is an integer then there might be two partitions, one for records with key less than 0 and one
for records with key greater than or equal to 0. Each record exists in one and only one partition. Partitions are
closed on the left, and open on the right, i.e. the minimum value of a partition is contained within the partition,
but the maximum value is not. For example if the row key field is a string then there could be two partitions, one
for strings up to, but not including 'G', and one for strings 'G' and greater. The first partition would be written
["", "G") and the second would be ["G", null). This notation indicates that the smallest possible value in the first
partition is the empty string, and that every string up to, but not including, "G" is in that partition. In the
second partition, the string "G" is the smallest possible string in that partition, and the null indicates that there
is no maximum value for strings in that partition.

## Partition splitting

Over time a partition may contain more records than a certain threshold and if this happens the partition will be
split into two child partitions. Over time this process builds up a tree of partitions. We think of this tree
upside-down, i.e. the tree grows downwards with the root at the top and the leaves at the bottom. The root partition
is the only partition with no parent. A leaf partition has no children.

The partition splitting process periodically checks for leaf partitions that have become larger than the specified
maximum partition size. The size of a partition can be determined from the state store as that records the number of
records in each file, and which partition a file is in. When a partition that is too large is found a partition
splitting job is created and sent to an SQS queue. When a job arrives on this queue, a lambda processes it. To split
the partition, the lambda identifies the midpoint of the partition. It then creates two child partitions, and calls
the state store to add these two new partitions and to change the status of the parent partition to be a non-leaf.
This update is done atomically, conditional on the parent partition still being marked as a leaf partition. This
atomic, conditional, update ensures that a partition cannot be split twice.

How is the midpoint of a partition identified? Whenever a file is written out, either during the ingest process or
as the result of a compaction, a sidecar quantiles sketch file is written. This quantiles sketch allows quick estimation
of the quantiles of the keys in the file. Quantile sketches from different files can be merged together. To identify
the midpoint of a partition, the list of active files is retrieved from the state store. The associated sketches
are read from S3, and merged together. Then the median is found and used as the split point. This approach is much
quicker than reading all the data in sorted order and stopping once half the data has been read.

The partition splitting stack has two parts. The first consists of a Cloudwatch rule that periodically executes
a lambda that runs `sleeper.splitter.FindPartitionsToSplitLambda`. For each table, this queries the state store
to find the leaf partitions and the active files. For each leaf partition it then calculates the number of records
and if that is greater than a threshold it sends a message to an SQS queue saying that this partition should be
split. The second part of the stack is the lambda that is triggered when a message arrives on the SQS queue. This
lambda executes `sleeper.splitter.SplitPartitionLambda`. This splits the partition using the process described in
the previous paragraph.

Note that this partition splitting process happens independently of other parts of Sleeper. For example, the ingest
process needs to write data to leaf partitions. But if one of those partitions is split during this ingest process,
it does not matter. The partition that is split still exists and can still receive data. Similarly, the code that
executes queries periodically queries the state store for the current partitions and files. It then executes queries
by reading the appropriate files. If compactions finish or partitions are split in the time between the query executor
getting the partitions and files from the state store and the query being executed, it does not matter. It does not
make any difference to the results.

## State store

The state store for a table holds information about the files of data that are currently in the table, their status,
and how the data is partitioned. The state store allows information about the active files in a partition to
be retrieved, files to be added, a list of all the partitions to be retrieved, etc. It also allows the results
of a compaction job to be atomically committed by creating a DynamoDB transaction in which the record about each
input file is moved from the active table to the ready for garbage collection table, and a new record for the
output file is added to the active table.

There are currently two state store implementations, one that stores the data in DynamoDB and one that stores it
in Parquet files in S3 with a lightweight consistency layer in DynamoDB.

## DynamoDB state store

The DynamoDB state store uses three DynamoDB tables to store the state of a table. There is one table for active
files, one for files that are ready for garbage collection (i.e., files that have been read as part of a compaction
job, and that can therefore be deleted), and one for information about the partitions in the system. For the tables
for the active and ready for garbage collection files, the partition key is simply the name of the file. For the
partition table, the partition key is simply the name of the partition. Updates to the state that need to be
executed atomically are wrapped in DynamoDB transactions. The number of items in a DynamoDB transaction is limited
to 25. This has implications for the number of files that can be read in a compaction job. When the job finishes,
the input files need to be moved from the active table to the ready for garbage collection, and either one or two
output files need to be written. This means that at most 11 files can be read by a compaction job if the DyanmoDB
state store is used.

## S3 state store

This state store stores the state of a table in Parquet files in S3, within the same bucket used to store the data
for the table. There is one file for information about the files, and one for the partitions. When an update happens
a new file is written. This new file contains the complete information about the state, i.e., it does not just
contain the updated information. As two processes may attempt to update the information simultaneously, there needs
to be a consistency mechanism to ensure that only one update can succeed. A table in DynamoDB is used as this
consistency layer.

## Ingest of data

To ingest data to a table, it is necessary to write files of sorted records. Each file should contain data for one
and only one partition. When these files have been written to S3, the state store needs to be updated. There are
two ways to do this: standard ingest and bulk import.

### Standard ingest

Standard ingest is performed by the `sleeper.ingest.IngestRecords` class. This performs ingest using the following
steps:

- A batch of records is read into memory.
- This batch is sorted in memory and then flushed to a local file.
- The above two steps are repeated until a certain number of records have been written locally.
- The state store is then queried for all leaf partitions.
- As the local files are all sorted, it is possible to run a streaming merge of these files to produce a sorted
  iterable of records. This sorted iterable is then used to write records to files in S3, with one file per leaf
  partition.
- Once these files have been written to S3 then the state store is updated.

Note that once the ingest process is ready to start writing data to the S3 data bucket, it queries the state
store for the current leaf partitions. It then writes data to files in these leaf partitions. During the process
of writing this data the partition splitting stack may decide to split a partition. But this does not effect
the correctness of the ingest process. The partitions that the ingest process is writing to still exist, and can
still receive data. The fact that some of those partitions may no longer be leaf partitions does not matter. All
that matters is that the ingest process writes files of data such that each file contains data for one and only
one partition.

Users can avoid the complexity of deploying and running multiple instances of the `IngestRecords` class by
writing the data that they wish to ingest into Parquet files and then sending a message to a queue telling Sleeper
to ingest that data. This then causes ECS tasks to run to perform the ingest. These tasks are calling the
`IngestRecords` class on an iterable of records that simply reads the Parquet files.

The resources that provide this functionality are deployed by the ingest stack. The user sends a message to
the SQS queue containing details of the files to be ingested. The ingest stack consists of the SQS queue to
which messages are sent, and the ECS cluster and Fargate task definition which will be used to execute the
ingest jobs. The number of Fargate tasks that are running scales down naturally as the tasks terminate if
there are no more messages on the SQS queue. To scale up the number of tasks, a Cloudwatch rule
periodically triggers a lambda. This lambda looks at the number of messages on the queue that are not being
processed and if necessary creates more Fargate tasks. The maximum number of concurrent Fargate tasks is configurable.

### Bulk import

A bulk import is a process of ingesting data into a Sleeper table using Spark to perform the partitioning
and sorting of the data. This Spark job runs on either an EMR or an EKS cluster (the latter is experimental).
There are two variations of the EMR-based bulk import. The first is an on-demand approach. The user sends a
message to a queue with a list of the files that they want to ingest. This triggers a lambda that creates an
EMR cluster to perform that ingest. As this cluster is created specifically for this job, the job needs to
contain a large number of records. The cluster will take around 10 minutes to create.

The other EMR-based approach uses a persistent, i.e. long running, EMR cluster. A process on the master node
of the cluster monitors a queue and when a job appears submits it to YARN for execution. The EMR cluster can
either be of fixed size or use EMR managed scaling.

### Ingest batcher

The ingest batcher groups ingest requests for individual files into ingest or bulk import jobs. File ingest requests are
submitted to an SQS queue. The batcher is then triggered periodically to group files into jobs and send them to the
ingest queue configured for the table. The number of jobs created is determined by the configuration of the batcher.

The files need to be accessible to the relevant ingest system, but are not read directly by the batcher. 

An outline of the design of this system is shown below:

![Ingest Batcher design diagram](diagrams/ingest-batcher.png)

## Compactions

The purpose of a compaction job is to read N files and replace them with one file. This process keeps the number
of files for a partition small, which means the number of files that need to be read in a query is small. The input
files are all from within one partition and contain records sorted by key and sort fields.
The output from the job is a sorted file. As the input files are sorted, it is simple to write out a sorted
file containing their data. There are two types of compaction job: non-splitting and splitting. A non-splitting job
is one in which the files are in a partition that is a leaf partition. In this case there is only one output file
and it is in the same partition as the input files. A splitting job is one in which the files are in a partition
that is not a leaf partition. In this case two output files are created, one for each of the child partitions.
Currently, there are separate queues, ECS clusters and lambdas for these two types of jobs, although there is
no intrinsic reason for them to be separate.

When a compaction job completes, it needs to update the state store to change the status of the input files to
ready-for-garbage-collection and it needs to add the output file(s) as active file(s). This update must be done
atomically, to avoid clients that are requesting the state of the state store from seeing an inconsistent view.

The CDK compaction stack deploys the infrastructure that is used to create and execute compaction jobs. A compaction
job reads in N input files and merges them into 1 or 2 output files. As the input files are all sorted by key, this
job is a simple streaming merge that requires negligible amounts of memory. The input files are all from a single
partition.

There are two separate stages: the creation of compaction jobs, and the execution of those jobs. Compaction jobs
are created by a lambda that runs the class `sleeper.compaction.job.creation.CreateJobsLambda`. This lambda is
triggered periodically by a Cloudwatch rule. The lambda iterates through each table. For each table, it queries
the state store for information about the partitions and about the active files that do not have a job id (if a file
has a job id it means that a compaction job has already been created for that file). It then uses a compaction
strategy to decide what compaction jobs should be created. The compaction strategy can be configured independently
for each table. Jobs that are created by the strategy are sent to an SQS queue.

Compaction jobs are executed in containers. Currently these containers are executed in Fargate tasks but they could
be executed on ECS running on EC2 instances, or anywhere that supports running Docker containers. These containers
retrieve compaction jobs from the SQS queue and execute them. Executing them involves a streaming merge of the
N input files into one sorted file. Once the job is finished, the state store is updated. The number of Fargate
tasks that are running scales down naturally as the task terminates if there are no more messages on the SQS queue.
To scale up the number of tasks, a Cloudwatch rule periodically triggers a lambda. This lambda looks at the number
of messages on the queue that are not being processed and if necessary creates more Fargate tasks. The maximum
number of concurrent compaction tasks is configurable.

## Garbage collection

A file is ready for garbage collection if it was marked as being ready for garbage collection more than N minutes
ago, where N is a parameter that can be configured separately for each table. The default value of N is 10 minutes.
The reason for not deleting the file immediately it is marked as ready for garbage collection is that it may be being
used by queries.

The garbage collector stack is responsible for deleting files that are ready for garbage collection. It consists of
a Cloudwatch rule that periodically triggers a lambda. This lambda iterates through all the tables. For each table
it queries the state store to retrieve all the files that have had a status of ready for garbage collection for
more than N minutes. These files are then deleted in batches.

## Queries

A Sleeper query is a request for all records where the key is in a range (or in one of a list of ranges). Queries
are executed by the `QueryExecutor` class. This contains a cache of the information required from the state store
(namely the partition tree and the active files). This cache is refreshed periodically. When a query is received,
the requested ranges are examined to see which leaf partitions overlap with the range. Then all partitions up
the partition tree from the leaf partition to the root are found. Records that are relevant to the query may be
found in any of these partitions. All the active files in these partitions are then found. Each file is opened
with a filter that specifies the required range. A streaming merge of the results is performed, with these results
being passed through the compaction and query time iterators.

If the ranges span multiple leaf partitions, then the query is split up into multiple sub-queries, one per leaf
partition.

Queries can be executed directly from Java. Sleeper also allows queries to be executed in
lambdas. Queries are sent to an SQS queue. A lambda picks up this message. If the query only overlaps one partition
then it is executed by that lambda. Otherwise the query is broken into subqueries that are placed back on the queue
so that they can be executed in parallel.

## Athena integration

Sleeper allows Athena queries to be run over Sleeper data. This is enabled by the CDK Athena stack. It contains
lambda functions that read Sleeper data and pass it to Athena. This can be done in two ways: with or without the
application of the iterators.

## Iterators

An iterator is a function that is called either during a compaction job or during a query. It allows
logic to be inserted into the compaction or query path. This logic could be used to age-off old data or to
aggregate together values for the same key (e.g. to sum counts associated with the same key). Each iterator is a
function that takes as input a `CloseableIterator<Record>` and returns a `CloseableIterator<Record>`. Examples of
iterators can be found in `sleeper.core.iterator.impl`.
