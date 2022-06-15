Ingesting data
==============

The simplest way to ingest data into Sleeper is to write out the data you want to ingest as Parquet files
with columns matching the fields in your schema (note that the fields in the schema of the Parquet file
all need to be non-optional). There are then two ways to ingest the data: standard ingest and
bulk import. Both of these approaches need to partition the data according to the current partitioning in the
Sleeper table and then sort the data within those partitions. The data needs to be written to Parquet files and then
the state store needs to be updated. Note that all ingest into Sleeper is done in batches - there is currently
no option to ingest the data in a way that makes it immediately available to queries. There is a trade-off between
the latency of data being visible and the cost, with lower latency generally costing more.

Standard ingest uses ECS tasks to ingest the data. Each task partitions and sorts the data locally inside the
container before writing it to S3. To ingest large volumes of data using standard ingest, it is necessary to
run many ECS tasks.

Bulk import refers to ingesting data using Spark jobs running on either EMR or EKS. This is potentially more
efficient than standard ingest as the number of files written is smaller, which means the cost for S3 PUTs is
less and there is less compaction work to do later.

It is also possible to ingest data directly from Java using the class `sleeper.ingest.IngestRecords`. The standard
ingest method is based on this.

Note that the descriptions below describe how data in Parquet files can be ingested by sending ingest job
definitions in JSON form to SQS queues. In practice it is easier to use the [Python API](08-python-api.md).

## Standard Ingest

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

Here the items listed under 'files' can be either files or directories. If they are directories, then Sleeper
will recursively look for files ending in '.parquet' within them.

It is up to you to spread the data you want to ingest over an appropriate number of jobs. As a general rule,
aim for at least 10s of millions of records per job.

The id field will be used in logging so that users can see the progress of particular ingest jobs by viewing the
logs. The URL of the SQS queue that the message should be sent to can be found from the `sleeper.ingest.job.queue.url`
property. This will be populated in the config object in the sleeper-<instance-id>-config S3 bucket.

You will need to ensure that the role with the ARN given by the IngestContainerRoleARN property has read access
to the files you wish to ingest. This ARN is exported as a named export from CloudFormation with name
<sleeper-id>-IngestContainerRoleARN to help stacks that depend on Sleeper automatically grant read access to their
data to Sleeper's ingest role. A simple way to do this is to use the sleeper.ingest.source.bucket instance property to
set the name of the bucket that the files are in. If this property is populated when the Sleeper instance is deployed then
the ingest roles will be granted read access to it. (The bulk import methods described below will also be granted read access
to it.)

Once the message has been sent to the SQS, a lambda will notice that there are messages on the queue and then start
a task on the ingest ECS cluster (this cluster is called `sleeper-<instance-id>-ingest-cluster`). This task will
then ingest the data. This process is asynchronous, i.e. it may be several minutes before the data has been ingested.

## Bulk Import

The bulk import stack allows data to be ingested using Apache Spark. This can be run on either EMR or EKS. The advantage
of bulk import over the standard ingest process described above is that it reduces the number of writes to S3. For example,
suppose there are currently 100 leaf partitions for a table, and suppose that we have 1000 files of data to ingest. With the
standard approach, if we create one ingest job per file and send it to the SQS queue, then there will be 100,000 writes to S3.
(In fact, there might be more if the files contain more records than the value of sleeper.ingest.max.local.records.) Using
the bulk import method, there will only be 100 writes to S3 (assuming that the 1000 files are all imported in the same bulk
import job).

There are several stacks that allow data to be imported using the bulk import process:

- EmrBulkImportStack - this causes an EMR cluster to be deployed each time a job appears on the bulk import queue. Each
job is processed on a separate EMR cluster. The advantage of the cluster being used for one job and then destroyed is
that there is no wasted compute if jobs come in infrequently. The downside is that there is a significant delay whilst
the cluster is created and bootstrapped.
- PersistentEmrBulkImportStack - this causes an EMR cluster to be created when the Sleeper instance is deployed. This
cluster runs continually. By default, it uses managed scaling so that the number of servers running scales up and down
as needed. The advantage of the persistent EMR approach is that if there is a continual stream of jobs coming there is no
delay while a new cluster is created (this also means the cost of the servers during the cluster creation and bootstrapping
process is amortised over multiple jobs). The downside is that if there are no jobs to perform then there is still a cost.
- EksBulkImportStack - this uses Spark running on an EKS cluster to bulk import the data. Currently, the executors run
as Fargate tasks. Future work will allow them to run on EC2 instances. This stack is experimental.

These can all be deployed independently of each other. The `sleeper.optional.stacks` instance property needs to include
`EmrBulkImportStack`, `PersistentEmrBulkImportStack` or `EksBulkImportStack` respectively.

#### Bulk import on EMR

The non-persistent EMR stack creates an EMR cluster when you want to run a job. When a job is
submitted a cluster is created with a small number of instances. EMR managed auto-scaling is enabled for this cluster.

A simple example of the message to send is:

```JSON
{
  "tableName": "my-table",
  "files": [
  	"my-bucket/my-files/"
  ]
}
```

This message needs to be sent to the queue with URL given by the value of the property `sleeper.bulk.import.emr.job.queue.url`.

You can configure the instance type of the nodes, as well as the initial and maximum number of core nodes in your cluster.
Default values of these can be specified in the instance properties. These can be overridden for each table by editing
the table properties. Alternatively they can be specified on a per-job basis by editing the "platformSpec" part of the
job specification:

```JSON
{
  "tableName": "my-table",
  "files": [
  	"my-bucket/my-files/"
  ],
  "sparkConf": {
  	"spark.task.cpus": "2"
  },
  "platformSpec": {
  	"sleeper.table.bulk.import.emr.master.instance.type": "m5.xlarge",
  	"sleeper.table.bulk.import.emr.executor.instance.type": "m5.4xlarge",
  	"sleeper.table.bulk.import.emr.executor.initial.instances": "2",
  	"sleeper.table.bulk.import.emr.executor.max.instances": "10"
  }
}
```

When you submit your json job via the SQS Queue, an EMR cluster should appear in the EMR part of the AWS console with your
desired configuration. Once the cluster initialises (around 10 minutes), you will be able to follow the links in the EMR
console to access your Spark UI and application master UI. These will allow you to monitor your job and view logs from the
Spark executors and driver. After your job finishes the cluster terminates.

The bulk import using EMR approach has the following configuration options in the instance properties. These values will be
used unless they are overridden by either the table properties or the specification in the bulk import job specification.

```properties
sleeper.bulk.import.keypair.name=my-key # An EC2 keypair to use for the EC2 instances. Specifying this will allow you to SSH to the nodes in the cluster while it's running. (NB. This cannot be overriden by a table property or by the specification in the bulk import job specification.)
sleeper.default.bulk.import.emr.release.label=emr-6.4.0 # The EMR release label to be used when creating an EMR cluster for bulk importing data using Spark running on EMR. This default can be overridden by a table property or by a property in the bulk import job specification.
sleeper.default.bulk.import.emr.master.instance.type=m5.xlarge # The EC2 instance type to be used for the master node of the EMR cluster.
sleeper.default.bulk.import.emr.executor.instance.type=m5.4xlarge # The EC2 instance type to be used for the executor nodes of the EMR cluster.
sleeper.default.bulk.import.emr.executor.initial.instances=2 # The initial number of EC2 instances to be used as executors in the EMR cluster.
sleeper.default.bulk.import.emr.executor.max.instances=10 # The maximum number of EC2 instances to be used as executors in the EMR cluster.
sleeper.default.bulk.import.spark.shuffle.mapStatus.compression.codec=lz4 # This is used to set the value of spark.shuffle.mapStatus.compression.codec on the Spark configuration for bulk import jobs. Setting this to "lz4" stops "Decompression error: Version not supported" errors - only a value of "lz4" has been tested. This default can be overridden by a table property or by a property in the bulk import job specification.
sleeper.default.bulk.import.spark.speculation=true # This is used to set the value of spark.speculation on the Spark configuration for bulk import jobs. This default can be overridden by a table property or by a property in the bulk import job specification.
sleeper.default.bulk.import.spark.speculation.quantile=0.5 # This is used to set the value of spark.speculation.quantile on the Spark configuration. Lowering this from the default 0.75 allows us to try re-running hanging tasks sooner. This default can be overridden by a table property or by a property in the bulk import job specification.
```

The following options can be specified in the table properties. For jobs importing data to a particular table these values
will be used instead of the default values in the instance properties, unless the values in the table properties are overridden
by properties in the job specification.

```properties
sleeper.table.bulk.import.emr.release.label=emr-6.4.0 # The EMR release label to be used when creating an EMR cluster for bulk importing data using Spark running on EMR. This value overrides the default value in the instance properties. It can be overridden by a value in the bulk import job specification.
sleeper.table.bulk.import.emr.master.instance.type=m5.xlarge # The EC2 instance type to be used for the master node of the EMR cluster. This value 
overrides the default value in the instance properties. It can be overridden by a value in the bulk import job specification.
sleeper.table.bulk.import.emr.executor.instance.type=m5.4xlarge # The EC2 instance type to be used for the executor nodes of the EMR cluster. This value overrides the default value in the instance properties. It can be overridden by a value in the bulk import job specification.
sleeper.table.bulk.import.emr.executor.initial.instances=2 # The initial number of EC2 instances to be used as executors in the EMR cluster. This value overrides the default value in the instance properties. It can be overridden by a value in the bulk import job specification.
sleeper.table.bulk.import.emr.executor.max.instances=10 # The maximum number of EC2 instances to be used as executors in the EMR cluster. This value overrides the default value in the instance properties. It can be overridden by a value in the bulk import job specification.
sleeper.table.bulk.import.spark.shuffle.mapStatus.compression.codec=lz4 # This is used to set the value of spark.shuffle.mapStatus.compression.codec on the Spark configuration for bulk import jobs. Setting this to "lz4" stops "Decompression error: Version not supported" errors - only a value of "lz4" has been tested. This value overrides the default value in the instance properties. It can be overridden by a value in the bulk import job specification.
sleeper.table.bulk.import.spark.speculation=true # This is used to set the value of spark.speculation on the Spark configuration for bulk import jobs. This value overrides the default value in the instance properties. It can be overridden by a value in the bulk import job specification.
sleeper.table.bulk.import.spark.speculation.quantile=0.5 # This is used to set the value of spark.speculation.quantile on the Spark configuration. Lowering this from the default 0.75 allows us to try re-running hanging tasks sooner. This value overrides the default value in the instance properties. It can be overridden by a value in the bulk import job specification.
```

#### Bulk import on persistent EMR

The persistent EMR stack creates an EMR cluster when the Sleeper instance is deployed. Note that this will be running until
you terminate it. By default, EMR managed auto-scaling is enabled for this cluster. The `sleeper.bulk.import.persistent.emr.use.managed.scaling`
instance property determines whether managed scaling is used for this cluster. If this is set to false then the number of executors
in the cluster is set to `sleeper.bulk.import.persistent.emr.min.instances`. If it is set to true then the number of executors
automatically scales up and down between `sleeper.bulk.import.persistent.emr.min.instances` and `sleeper.bulk.import.persistent.emr.max.instances`.

The other properties of the cluster are controlled using similar properties to the non-persistent EMR cluster, e.g.

```properties
sleeper.bulk.import.persistent.emr.release.label=emr-6.4.0
sleeper.bulk.import.persistent.emr.master.instance.type=m5.xlarge
sleeper.bulk.import.persistent.emr.core.instance.type=m5.4xlarge
sleeper.bulk.import.persistent.emr.use.managed.scaling=true
sleeper.bulk.import.persistent.emr.min.instances=1
sleeper.bulk.import.persistent.emr.max.instances=10
```

The URL of the SQS queue to which messages should be sent is given by the instance property
`sleeper.bulk.import.persistent.emr.job.queue.url`which can be found in the config
object in the bucket named 'sleeper-<instance-id>-config'.

Note however that as there is one persistent EMR cluster deployed for the whole instance there are no per-table persistent
EMR properties, and it does not make sense to change the cluster properties on a per-job basis.


#### Bulk import on EKS

The EksBulkImportStack option requires the bulk import Docker image to be pushed to ECR - see the instructions in the [deployment guide](02-deployment-guide.md).


You can submit a job in a similar way to the methods above, e.g.

```JSON
{
    "tableName": "myTable",
    "files": [
        "my-import-bucket/files/example.parquet",
        "my-import-bucket/files/my-other-files/"
    ],
    "sparkConf": {
        "spark.executor.instances": "3",
        "spark.driver.memory": "7g",
        "spark.driver.memoryOverhead": "1g",
        "spark.executor.memory": "7g",
        "spark.executor.memoryOverhead": "1g"
        "spark.driver.cores": "1",
        "spark.executor.cores": "1"
    }
}
```

You can change the memory settings and number of executors. The settings shown are the default ones so will be
included even if you don't ask for them. It's important that your driver and executor memory and CPU settings are
compatible with [AWS Fargate's supported values](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html)
otherwise the job will fail to start. The total memory for the spark driver or executor is calculated by adding
the `spark.[driver/executor].memory` and `spark.[driver/executor].memoryOverhead` The memory overhead should be
around 10% of the executor memory. Otherwise you start to run into memory issues on Kubernetes and your nodes
will start being killed.

The Spark job will be run with a service account which has permissions to read from the bulk import bucket, and write to all the
Sleeper tables in the instance so there should be no issues with permissions.

The bulk import job will go through some initial validation, and if successful will be transformed and submitted to an AWS Step
Functions StateMachine. If the job fails validation, or for some reason is unable to be submitted to the StateMachine, a CloudWatch
alarm will trigger and an email will be sent to the address specified in `sleeper.errors.email`.

When the job makes it to the StateMachine, it will run the job synchronously and watch its status. If the job is successful, the
job will be torn down automatically. If unsuccessful or the job doesn't submit, a notification will be sent to the errors email.

##### Debugging and UI access

While a spark job is running you'll be able to monitor it with the Spark UI. To access this, you'll need to install kubectl, a command line
utility for Kubernetes. Once you've done that, have a look at the outputs of the BulkImportStack. There should be one with a value like:
`BulkImportStack.BulkImportClusterConfigCommandABCD1234 = aws eks update-kubeconfig --name ...`. Copy and paste this command into a terminal.
This will give you access to your cluster. From there you'll be able to inspect logs, list pods and connect remotely to the Spark UI. The driver
pods all use the job ID as it's name. If you don't set this manually, it will be a random UUID.

```bash
instance_id=abc1234

# This shortcut means we don't have to add -n <the namespace> to all our commands
kubectl config set-context --current --namespace sleeper-${instance_id}-bulk-import

# Inspect the logs (add -f to follow them)
kubectl logs pods/my-job-name

# Forward connections to the spark UI (type localhost:4040 into your browser to bring it up)
kubectl port-forward my-job-name 4040:4040
```

