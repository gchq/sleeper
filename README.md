Sleeper
=======

## Introduction

Sleeper is a serverless, cloud-native, log-structured merge tree based, scalable key-value store. It is designed to
allow the ingest of very large volumes of data at low cost. Data is stored in rows in tables. Each row has a key field,
and an optional sort field, and some value fields. Queries for rows where the key takes a given value takes around
1-2 seconds, but many thousands can be run in parallel. Each individual query has a negligible cost.

Sleeper can be thought of as a cloud-native reimagining of systems such as Hbase and Accumulo. The architecture is
very different to those systems. Sleeper has no long running servers. This means that if there is no work to be done,
i.e. no data is being ingested and no background operations such as compactions are in progress, then the only cost
is the cost of the storage. There are no wasted compute cycles, i.e. it is "serverless".

The current codebase can only be deployed to AWS, but there is nothing in the design that limits it to AWS. In time
we would like to be able to deploy Sleeper to other public cloud environments such as Microsoft Azure
or to a Kubernetes cluster.

Note that Sleeper is currently a prototype. Further development and testing is needed before it can be considered
to be ready for production use.

## Functionality

Sleeper stores records in tables. A table is a collection of records that conform to a schema. A record is a map
from a field name to value. For example, a schema might have a row key field called 'id' of type string, a sort
field called 'timestamp' of type long, and a value field called 'name' of type string. Each record in a table
with that schema is a map with keys of id, timestamp and name. Data in the table is stored range-partitioned by
the key field. Within partitions, records are stored in Parquet files in S3. These files contain records in sorted
order (sorted by the key field and then by the sort field).

Sleeper is deployed using CDK. Each bit of functionality is deployed using a separate CDK substack of one main
stack.

- State store stacks: Each table has a state store that stores metadata about the table such as files 
  that are in the table, the partitions they are in and information about the partitions themselves.
- Compaction stack: As the number of files in a partition increases, their contents must be merged 
  ("compacted") into a single sorted file. The compaction stack performs this task using lambda and SQS for job
  creation, queueing and Fargate/EC2 for execution of the tasks.
- Garbage collector stack: After compaction jobs have completed, the input files are deleted (after a user
  configurable delay to ensure the files are not in use by queries). The garbage collector stack performs this
  task using a lambda function.
- Partition splitting stack: Partitions need to be split once they reach a user configurable size. This only
  affects the metadata about partitions in the state store. This task is performed using a lambda.
- Ingest stack: This allows the ingestion of data from Parquet files. These files need to consist of data that
  matches the schema of the table. They do not need to be sorted or partitioned in any way. Ingest job requests
  are submitted to an SQS queue. They are then executed by tasks running on an ECS cluster. These tasks are scaled
  up using a lambda that periodically monitors the number of items on the queue. The tasks scale down naturally
  as if there are no jobs on the queue a task will terminate.
- Query stack: This stack allows queries to be executed in lambdas. Queries are submitted to an SQS queue. This
  triggers a lambda that executes the queries. It can then write the results to files in an S3 bucket, or send the
  results to an SQS queue. A query can also be executed from a websocket client. In this case the results will be
  returned directly to a client. Note that queries can also be submitted directly from a Java client, and this does
  not require the query stack.
- Topic stack: Notifications of errors and failures will appear on an SNS topic.
- EMR bulk import stack: This allows large volumes of data in Parquet files to be ingested. A bulk import job
  request is sent to an SQS queue. This triggers a lambda that creates an EMR cluster. This cluster runs a Spark
  job to import the data. Once the job is finished the EMR cluster shuts down.
- Persistent EMR bulk import stack: Similar to the above stack, but the EMR cluster is persistent, i.e. it never
  shuts down. This is appropriate if there is a steady stream of import jobs. The cluster can either be of fixed
  size or it can use EMR managed scaling.
- EMR Serverless Bulk Import stack: Similar to the above 2 stacks in behaviour. This stack is created at Sleeper 
  instance deployment. It is the default way in which the bulk imports are run and provides benefit by the fact that 
  when no bulk import jobs are present, no computing resources are used.
- Dashboard stack: This displays properties of the system in a Cloudwatch dashboard.

The following functionality is experimental:

- Athena: There is experimental integration with Athena to allow SQL analytics to be run against a table.
- Trino: There is a plugin for Trino that allows SQL select, join and insert statements to be run against multiple
  Sleeper tables. It returns results quickly and it is particularly suitable for interactive queries which retrieve just
  a few rows from the underlying Sleeper instance, such as SELECT * FROM t1 INNER JOIN t2 USING (keycol) WHERE t1.name =
  "Fred Jones"
- Bulk import using Spark running on EKS: This stack allows data to be ingested by running Spark on EKS. Currently
  this runs the Spark executors in either Fargate or EC2.

Sleeper provides the tools to implement fine-grained security on the data, although further work is needed to make
these easier to use. Briefly, the following steps are required:

- Decide how to store the security information in the table, e.g. there might be one security label per row,
  or two per row, or one per cell. These fields must be added to the schema.
- Write an iterator that will run on the results of every query to filter out records that a user is not permitted
  to see. This takes as input a user's authorisations and uses those to make a decision as to whether the user can see
  the data.
- Ensure the Sleeper instance is deployed such that the boundary of the system is protected.
- Ensure that queries are submitted to the query queue via a service that authenticates users, and passes their
  authorisations into the query time iterator configuration.

## License

Sleeper is licensed under the [Apache 2](http://www.apache.org/licenses/LICENSE-2.0) license.

## Documentation

See the documentation contained in the docs folder:

1. [Getting started](docs/01-getting-started.md)
2. [Deployment guide](docs/02-deployment-guide.md)
3. [Creating a schema](docs/03-schema.md)
4. [Tables](docs/04-tables.md)
5. [Ingesting data](docs/05-ingest.md)
6. [Checking the status of the system](docs/06-status.md)
7. [Retrieving data](docs/07-data-retrieval.md)
8. [Python API](docs/08-python-api.md)
9. [Trino](docs/09-trino.md)
10. [Deploying to LocalStack](docs/10-deploy-to-localstack.md)
11. [Developer guide](docs/11-dev-guide.md)
12. [Dependency conflicts](docs/12-dependency-conflicts.md)
13. [Design](docs/13-design.md)
14. [System tests](docs/14-system-tests.md)
15. [Release process](docs/15-release-process.md)
16. [Common problems and their solutions](docs/16-common-problems-and-their-solutions.md)
17. [Roadmap](docs/17-roadmap.md)
