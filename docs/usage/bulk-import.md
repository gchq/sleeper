Bulk Import
===========

Bulk importing data into a Sleeper table means importing data by using Apache Spark to run a MapReduce-like job to
take a batch of data then partition, sort and write it out so that the resulting files can be added into a Sleeper
table. One advantage of bulk import over the [standard ingest process](standard-ingest.md) is that it reduces the number
of writes to S3.

For example, suppose there are currently 100 leaf partitions for a table, and suppose that we have 1000
files of data to ingest. With the standard approach, if we create one ingest job per file and send it to the SQS queue,
then there will be 100,000 writes to S3 (in fact, there might be more if the files contain more rows than the value
of `sleeper.ingest.max.local.rows`). Using the bulk import method, there will only be 100 writes to S3 (assuming that
the 1000 files are all imported in the same bulk import job).

Note that it is vital that a table's partitions are split based on a representative sample of data. Bulk import requires
a minimum number of partitions, set in the table property `sleeper.table.bulk.import.min.leaf.partitions`,
documented [here](properties/table/bulk_import.md). If the table is not pre-split as
described [here](../usage/tables.md#pre-split-partitions), or if too few partitions are present, then when
a bulk import job is submitted the partitions will be split automatically, based on the data in the bulk import job.
This will assume that the job's data is a representative sample for the table.

Files to be ingested must be accessible to the EMR cluster, and to the lambda that receives the job to submit to the
cluster. See the [ingest guide](ingest.md#prepare-files) for how to prepare your files for access.

There are several stacks that allow data to be imported using the bulk import process:

- `EmrServerlessBulkImportStack` - this causes an EMR Serverless application to be created when the Sleeper instance is deployed.
  This is the default EMR Bulk Import Stack. The advantage of using EMR Serverless is that when there are no bulk import jobs
  the applications stops with no wasted compute. The startup of the application is greatly reduced compared to standard EMR.
- `EmrBulkImportStack` - this causes an EMR cluster to be deployed each time a job is submitted to the EMR bulk import
  queue. Each job is processed on a separate EMR cluster. The advantage of the cluster being used for one job and then
  destroyed is that there is no wasted compute if jobs are submitted infrequently. The downside is that there is a
  significant delay whilst the cluster is created and bootstrapped.
- `PersistentEmrBulkImportStack` - this causes an EMR cluster to be created when the Sleeper instance is deployed. This
  cluster runs continually. By default, it uses managed scaling so that the number of servers running scales up and down
  as needed. The advantage of the persistent EMR approach is that if there is a continual stream of jobs coming there is
  no delay while a new cluster is created (this also means the cost of the servers during the cluster creation and
  bootstrapping process is amortised over multiple jobs). The downside is that if there are no jobs to perform then
  there is still a cost.
- `EksBulkImportStack` - this uses Spark running on an EKS cluster to bulk import the data. Currently, the executors run
  as Fargate tasks. Future work will allow them to run on EC2 instances. This stack is experimental.

These can all be deployed independently of each other. Each stack has its own queue from which it pulls jobs. The
`sleeper.optional.stacks` instance property needs to include `EmrServerlessBulkImportStack`, `EmrBulkImportStack`,
`PersistentEmrBulkImportStack` or `EksBulkImportStack` respectively.

If you have occasional bulk import jobs, or you just want to get started, then we recommend the serverless EMR approach.
If you will have a lot of jobs running fairly constantly, then the persistent EMR approach is recommended.

#### Bulk import on EMR Serverless

The EMR Serverless stack creates an EMR Serverless application that only runs when there are jobs to process.
When you want to run a job the application is started by EMR Serverless. After 15 minutes of inactivity the application
is shutdown ready to be started when needed. This can be overridden by changing the value of `sleeper.bulk.import.emr.serverless.autostop.timeout`

A simple example of the message to send is:

```JSON
{
  "tableName": "my-table",
  "files": [
    "my-bucket/my-files/"
  ]
}
```

This message needs to be sent to the queue with URL given by the value of the
property `sleeper.bulk.import.emr.serverless.job.queue.url`.

When you submit your JSON job via the SQS Queue, an EMR Serverless job should appear in the application found in the EMR
Studio part of the AWS console with your desired configuration. Once the job starts (around 2 minutes), you will be able
to follow the links in EMR Studio to access your Spark UI. This will allow you to monitor your job and view logs from
the Spark executors and driver. You can also access previous job Spark UIs from EMR Studio.

It is possible to get Sleeper to deploy EMR Studio by enabling the optional stack `EmrStudioStack`.
Note if EMR Serverless is not enabled then EMR Studio won't be deployed even if added to the optional stacks.

After your job finishes the application will auto shutdown after 15 minutes. When in the stopped state it takes seconds for the application to start when a new job is received.

The following can be edited in the Sleeper Admin console. It it also possible to set these on a per job basis by setting `sparkConf`

An example for overriding at the job level is:

```JSON
{
  "tableName": "my-table",
  "files": [
    "my-bucket/my-files/"
  ],
  "sparkConf": {
    "sleeper.bulk.import.emr.serverless.spark.emr-serverless.executor.disk": "120G",
    "sleeper.bulk.import.emr.serverless.spark.executor.instances": "25",
    "sleeper.bulk.import.emr.serverless.spark.driver.cores": "4",
    "sleeper.bulk.import.emr.serverless.spark.driver.memory": "8G"
  }
}
```
##### All Available Properties

```properties
# The following properties are used to define the custom Spark image used that has Java 11 installed
sleeper.bulk.import.emr.serverless.repo=<insert-unique-sleeper-id>/bulk-import-runner-emr-serverless
sleeper.bulk.import.emr.serverless.java.home=/usr/lib/jvm/jre-11

# The following properties define the executor and driver configuration
sleeper.bulk.import.emr.serverless.executor.cores=4
sleeper.bulk.import.emr.serverless.executor.memory=16g
sleeper.bulk.import.emr.serverless.executor.disk=200g
sleeper.bulk.import.emr.serverless.executor.instances=36
sleeper.bulk.import.emr.serverless.driver.cores=4
sleeper.bulk.import.emr.serverless.driver.memory=16g
sleeper.bulk.import.emr.serverless.dynamic.allocation.enabled=false

# The following properties configure how Spark will function
sleeper.bulk.import.emr.serverless.spark.rdd.compress=true
sleeper.bulk.import.emr.serverless.spark.shuffle.compress=true
sleeper.bulk.import.emr.serverless.spark.shuffle.spill.compress=true
sleeper.bulk.import.emr.serverless.spark.default.parallelism=288
sleeper.bulk.import.emr.serverless.spark.sql.shuffle.partitions=288
sleeper.bulk.import.emr.serverless.spark.network.timeout=800s
sleeper.bulk.import.emr.serverless.spark.executor.heartbeat.interval=60s
sleeper.bulk.import.emr.serverless.spark.memory.fraction=0.80
sleeper.bulk.import.emr.serverless.spark.memory.storage.fraction=0.30
sleeper.bulk.import.emr.serverless.spark.speculation=false
sleeper.bulk.import.emr.serverless.spark.speculation.quantile=0.75
sleeper.bulk.import.emr.serverless.spark.shuffle.mapStatus.compression.codec=lz4
```

##### Pre-initialised Capacity
EMR Serverless can be configured to have a pre-initialised capacity where resources are ready to process jobs.
This does incur an additional cost when the Application is not in the CREATED or STOPPED states.

Spark adds a 10% memory overhead to the drivers and executors which needs to be factored in to the resource requested.

See [here](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/pre-init-capacity.html)
for more information.

By default the pre-initialised capacity is disabled.

To enable it set `sleeper.bulk.import.emr.serverless.initial.capacity.enabled`

The configuration properties for pre-initialised capacity are:

```properties
sleeper.bulk.import.emr.serverless.initial.capacity.enabled=false
sleeper.bulk.import.emr.serverless.initial.capacity.executor.count=25
sleeper.bulk.import.emr.serverless.initial.capacity.executor.cores=4vCPU
sleeper.bulk.import.emr.serverless.initial.capacity.executor.memory=18GB
sleeper.bulk.import.emr.serverless.initial.capacity.executor.disk=200GB
sleeper.bulk.import.emr.serverless.initial.capacity.driver.count=2
sleeper.bulk.import.emr.serverless.initial.capacity.driver.cores=4vCPU
sleeper.bulk.import.emr.serverless.initial.capacity.driver.memory=18GB
sleeper.bulk.import.emr.serverless.initial.capacity.driver.disk=20GB
```
When pre-initialised capacity is turned on it is recommend to ensure that auto stop is also enabled by setting `sleeper.bulk.import.emr.serverless.autostop.enabled`.

This is to release the resources once jobs have finished thus reducing the overall cost of using EMR Serverless.

More information about EMR Serverless can be found [here](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html).

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

This message needs to be sent to the queue with URL given by the value of the
property `sleeper.bulk.import.emr.job.queue.url`.

You can configure the instance type of the nodes, as well as the initial and maximum number of core nodes in your
cluster. Default values of these can be specified in the instance properties. These can be overridden for each table by
editing the table properties. Alternatively they can be specified on a per-job basis by editing the `platformSpec` part
of the job specification:

```JSON
{
  "tableName": "my-table",
  "files": [
    "my-bucket/my-files/"
  ],
  "platformSpec": {
    "sleeper.table.bulk.import.emr.instance.architecture": "x86_64",
    "sleeper.table.bulk.import.emr.master.x86.instance.types": "m6i.xlarge",
    "sleeper.table.bulk.import.emr.executor.x86.instance.types": "m6i.4xlarge",
    "sleeper.table.bulk.import.emr.executor.initial.instances": "2",
    "sleeper.table.bulk.import.emr.executor.max.instances": "10"
  }
}
```

When you submit your JSON job via the SQS Queue, an EMR cluster should appear in the EMR part of the AWS console with
your desired configuration. Once the cluster initialises (around 10 minutes), you will be able to follow the links in
the EMR console to access your Spark UI and application master UI. These will allow you to monitor your job and view
logs from the Spark executors and driver. After your job finishes the cluster terminates.

There are many configuration options that can be specified to control properties of the EMR cluster and the Spark
configuration. The following properties are instance properties that can be overridden by table properties and by using
the `platformSpec` part of the job specification:

```properties
sleeper.default.bulk.import.emr.release.label=emr-6.10.0 # The EMR release label to be used when creating an EMR cluster for bulk importing data using Spark running on EMR. This default can be overridden by a table property or by a property in the bulk import job specification.
sleeper.default.bulk.import.emr.instance.architecture=x86_64 # The architectures to use for instances of the cluster. This determines which instance type properties will be read.
sleeper.default.bulk.import.emr.master.x86.instance.types=m6i.xlarge # The EC2 x86_64 instance types to be used for the master node of the EMR cluster.
sleeper.default.bulk.import.emr.executor.x86.instance.types=m6i.4xlarge # The EC2 x86_64 instance types to be used for the executor nodes of the EMR cluster.
sleeper.default.bulk.import.emr.executor.initial.instances=2 # The initial number of capacity units to provision as EC2 instances for executors in the EMR cluster.
sleeper.default.bulk.import.emr.executor.max.instances=10 # The maximum number of capacity units to provision as EC2 instances for executors in the EMR cluster.
```

The following options can be specified in the table properties. For jobs importing data to a particular table these
values will be used instead of the default values in the instance properties, unless the values in the table properties
are overridden by properties in the job specification.

```properties
sleeper.table.bulk.import.emr.release.label=emr-6.10.0 # The EMR release label to be used when creating an EMR cluster for bulk importing data using Spark running on EMR. This value overrides the default value in the instance properties. It can be overridden by a value in the bulk import job specification.
sleeper.table.bulk.import.emr.instance.architecture=x86_64 # The architectures to use for instances of the cluster. This determines which instance type properties will be read.
sleeper.table.bulk.import.emr.master.x86.instance.types=m6i.xlarge # The EC2 instance types to be used for the master node of the EMR cluster. This value overrides the default value in the instance properties. It can be overridden by a value in the bulk import job specification.
sleeper.table.bulk.import.emr.executor.x86.instance.types=m6i.4xlarge # The EC2 instance types to be used for the executor nodes of the EMR cluster. This value overrides the default value in the instance properties. It can be overridden by a value in the bulk import job specification.
sleeper.table.bulk.import.emr.executor.initial.instances=2 # The initial number of capacity units to provision as EC2 instances for executors in the EMR cluster. This value overrides the default value in the instance properties. It can be overridden by a value in the bulk import job specification.
sleeper.table.bulk.import.emr.executor.max.instances=10 # The maximum number of capacity units to provision as EC2 instances for executors in the EMR cluster. This value overrides the default value in the instance properties. It can be overridden by a value in the bulk import job specification.
```

##### Instance types

You can define the default instance types that the master node and executor node use with the following
architecture-specific properties. Each property can also be overridden in the table properties or in the bulk import job
specification as demonstrated above.

```properties
# The following properties define which architecture's instance types will be used (can be "x86_64" "arm64" or "x86_64,arm64")
sleeper.default.bulk.import.emr.instance.architecture=x86_64
sleeper.bulk.import.persistent.emr.instance.architecture=x86_64

# The following properties are specific to x86_64 instance types
sleeper.default.bulk.import.emr.master.x86.instance.types=m6i.xlarge # The EC2 x86_64 instance types to be used for the master node of the EMR cluster.
sleeper.default.bulk.import.emr.executor.x86.instance.types=m6i.4xlarge # The EC2 x86_64 instance types to be used for the executor nodes of the EMR cluster.
sleeper.bulk.import.persistent.emr.master.x86.instance.types=m6i.xlarge # The EC2 x86_64 instance types to be used for the master node of the EMR cluster.
sleeper.bulk.import.persistent.emr.executor.x86.instance.types=m6i.4xlarge # The EC2 x86_64 instance types to be used for the executor nodes of the EMR cluster.

# The following properties are specific to ARM64 instance types
sleeper.default.bulk.import.emr.master.arm.instance.types=m6g.xlarge # The EC2 ARM64 instance types to be used for the master node of the EMR cluster.
sleeper.default.bulk.import.emr.executor.arm.instance.types=m6g.4xlarge # The EC2 ARM64 instance types to be used for the executor nodes of the EMR cluster.
sleeper.bulk.import.persistent.emr.master.arm.instance.types=m6g.xlarge # The EC2 ARM64 instance types to be used for the master node of the EMR cluster.
sleeper.bulk.import.persistent.emr.executor.arm.instance.types=m6g.4xlarge # The EC2 ARM64 instance types to be used for the executor nodes of the EMR cluster.
```

Multiple instance types can be specified separated by commas. Instances will be chosen depending on the capacity
available.

For executor nodes, you can assign weights to instance types to define the amount of capacity that each instance type
provides. By default, each instance type delivers a capacity of 1. You can set custom weights for an instance type by
adding a number after the instance type in this comma separated list. This must be a whole number.

For example:

```properties
sleeper.default.bulk.import.emr.executor.x86.instance.types=m6i.4xlarge,4,m6i.xlarge
```

The above configuration would tell EMR that an m6i.4xlarge instance would provide 4 times the capacity of an m6i.xlarge
instance. The m6i.xlarge instance type does not have a weight, so is defaulted to 1. In this example, if you set the
initial executor capacity to 3, EMR could fulfil that with one instance of m6i.4xlarge, or 3 instances of m6i.xlarge.

More information about instance fleet options can be
found [here](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-instance-fleet.html#emr-instance-fleet-options).

#### Bulk import on persistent EMR

The persistent EMR stack creates an EMR cluster when the Sleeper instance is deployed. Bulk import jobs are run as EMR
steps. Note that this cluster will be running until you terminate it by updating the instance properties and re-runnnig
the cdk deploy.

By default, EMR managed auto-scaling is enabled for this cluster.
The `sleeper.bulk.import.persistent.emr.use.managed.scaling` instance property determines whether managed scaling is
used for this cluster. If this is set to false then the number of executors in the cluster is set
to `sleeper.bulk.import.persistent.emr.min.instances`. If it is set to true then the number of executors automatically
scales up and down between `sleeper.bulk.import.persistent.emr.min.instances`
and `sleeper.bulk.import.persistent.emr.max.instances`.

The other properties of the cluster are controlled using similar properties to the non-persistent EMR cluster, e.g.

```properties
sleeper.bulk.import.persistent.emr.release.label=emr-6.10.0
sleeper.bulk.import.persistent.emr.instance.architecture=x86_64
sleeper.bulk.import.persistent.emr.master.x86.instance.types=m6i.xlarge
sleeper.bulk.import.persistent.emr.executor.x86.instance.types=m6i.4xlarge
sleeper.bulk.import.persistent.emr.use.managed.scaling=true
sleeper.bulk.import.persistent.emr.min.instances=1
sleeper.bulk.import.persistent.emr.max.instances=10
```

There is an additional property `sleeper.bulk.import.persistent.emr.step.concurrency.level` that is not applicable to
the non-persistent EMR approach. This controls the number of steps that can run concurrently.

The URL of the SQS queue to which messages should be sent is given by the instance property
`sleeper.bulk.import.persistent.emr.job.queue.url`which can be found in the config
object in the bucket named `sleeper-<instance-id>-config`.

Note however that as there is one persistent EMR cluster deployed for the whole instance there are no per-table
persistent EMR properties, and it does not make sense to change the cluster properties on a per-job basis.

#### Instance properties common to the EMR and persistent EMR stacks

The following options are based
on https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/.

```properties
# spark.driver options
sleeper.bulk.import.emr.spark.driver.cores=5 # The number of cores allocated to the Spark driver. Used to set spark.driver.cores.
sleeper.bulk.import.emr.spark.driver.memory=16g # The memory allocated to the Spark driver. Used to set spark.driver.memory.
sleeper.bulk.import.emr.spark.driver.extra.java.options=-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p' # Used to set spark.driver.extraJavaOptions.

# spark.executor options
sleeper.bulk.import.emr.spark.executor.cores=5 # The number of cores allocated to the Spark executor. Used to set spark.executor.cores.
sleeper.bulk.import.emr.spark.executor.memory=16g # The memory allocated to a Spark executor. Used to set spark.executor.memory.
sleeper.bulk.import.emr.spark.executor.heartbeat.interval=60s # Used to set spark.executor.heartbeatInterval.
sleeper.bulk.import.emr.spark.executor.instances=29 # The number of Spark executors. Used to set spark.executor.instances.
sleeper.bulk.import.emr.spark.executor.extra.java.options=-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p' # Used to set spark.executor.extraJavaOptions.

# spark.yarn options
sleeper.bulk.import.emr.spark.yarn.executor.memory.overhead=2g # Used to set spark.yarn.executor.memoryOverhead
sleeper.bulk.import.emr.spark.yarn.driver.memory.overhead=2g # Used to set spark.yarn.driver.memoryOverhead
sleeper.bulk.import.emr.spark.yarn.scheduler.reporter.thread.max.failures=5 # Used to set spark.yarn.scheduler.reporterThread.maxFailures

# spark.dynamicAllocation option
sleeper.bulk.import.emr.spark.dynamic.allocation.enabled=false # Used to set spark.dynamicAllocation.enabled

# spark.default.parallelism option
sleeper.bulk.import.emr.spark.default.parallelism=290 # Used to set spark.default.parallelism

# spark.memory options
sleeper.bulk.import.emr.spark.memory.fraction=0.80 # Used to set spark.memory.fraction
sleeper.bulk.import.emr.spark.memory.storage.fraction=0.30 # Used to set spark.memory.storageFraction

# spark.network options
sleeper.bulk.import.emr.spark.network.timeout=800s # Used to set spark.network.timeout
sleeper.bulk.import.emr.spark.storage.level=MEMORY_AND_DISK_SER # Used to set spark.storage.level
sleeper.bulk.import.emr.spark.rdd.compress=true # Used to set spark.rdd.compress
sleeper.bulk.import.emr.spark.shuffle.compress=true # Used to set spark.shuffle.compress
sleeper.bulk.import.emr.spark.shuffle.spill.compress=true # Used to set spark.shuffle.spill.compress

# spark.sql options
sleeper.bulk.import.emr.spark.sql.shuffle.partitions=290 # Used to set spark.sql.shuffle.partitions
```

#### Accessing YARN web interface and Spark application UI on EMR and persistent EMR clusters

To access the YARN and Spark web interfaces whilst running bulk import jobs on either the EMR or persistent EMR
clusters, you need to set the following instance properties:

```properties
sleeper.bulk.import.emr.keypair.name=my-key # An EC2 keypair to use for the EC2 instances. Specifying this will allow you to SSH to the nodes in the cluster while it's running.
sleeper.bulk.import.emr.master.additional.security.group=my-group # An EC2 Security Group. This will be added to the list of security groups that are allowed to access the servers in the cluster.
```

You can then use the instructions [here](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-web-interfaces.html)
to access the pages. Also
see [this link](https://aws.amazon.com/premiumsupport/knowledge-center/spark-driver-logs-emr-cluster/) for instructions
on how to access the logs of the Spark driver.

#### An example of using bulk import

This section describes how a large volume of rows were bulk imported into Sleeper using the Persistent EMR stack. A
set of 10 billion rows was generated at random. These rows conformed to the schema used for the system tests, i.e.
a row key of type string, a sort key of type long, and a value of type string. The row key and the value are random
strings of length 10 with characters from the lower case alphabet a to z. The sort key is a random long in the range 0
to 10,000,000,000. The rows were stored in 10 Parquet files in a prefix in an S3 bucket (given as `mybucket/data/` in
the examples below).

The table was pre-split into 256 partitions.

A persistent EMR cluster consisting of 10 core nodes of instance type m6i.4xlarge and a master of type m6i.xlarge was
created by setting the following instance properties:

```properties
sleeper.bulk.import.persistent.emr.release.label=emr-6.10.0
sleeper.bulk.import.persistent.emr.master.x86.instance.types=m6i.xlarge
sleeper.bulk.import.persistent.emr.executor.x86.instance.types=m6i.4xlarge
sleeper.bulk.import.persistent.emr.use.managed.scaling=false
sleeper.bulk.import.persistent.emr.min.instances=10
sleeper.bulk.import.persistent.emr.step.concurrency.level=2
```

These are the default settings, with the exception of the managed scaling option.

A bulk import job was triggered by sending the following JSON to the SQS queue for the persistent EMR bulk import stack.
This queue will have the name `instance-id-BulkImportPersistentEMRQ`.

```JSON
{
  "id": "id1",
  "tableName": "my-table",
  "files": [
    "mybucket/data/"
  ]
}
```

This runs with the default settings of 29 Spark executors. The progress of the job can be tracked using the steps tab.
This job took 23 minutes to run. The rate of import would therefore be 626 billion rows per 24 hours, if a sequence
of these jobs was run.

To increase the rate of import, there are two options: increase the size of the cluster so that multiple jobs can run
simultaneously, or increase the size of the cluster so that a job completes quicker.

The size of the cluster was increased to 20 core servers, by using the console to manually adjust the number of servers.
The number of executors a job uses was increased to 59 using the sparkConf section of the bulk import job
specification (see the section below). The job now takes 12 minutes per job. Ideally, doubling the size of the cluster
would cause the job to take half the time to run. In this case it is a little worse than half of 23 minutes, due to some
overhead in starting the job up. We can see that increasing the size of the cluster has the advantage of reducing the
latency.

The size of the cluster was next increased to 40 servers. Two jobs were run in parallel using the console to set the
number of concurrent steps to 2. Each job still takes 12 minutes to run. This would give an ingest rate of 2 *
10,000,000,000 * (24 * 60) / 12 = 2.4 trillion rows per day.

#### Overriding Spark's properties for a job

The Spark properties for a job can be overridden by specifying the `sparkConf` section of the job, e.g.

```JSON
{
  "id": "id1",
  "tableName": "my-table",
  "files": [
    "mybucket/data/"
  ],
  "sparkConf": {
    "spark.executor.instances": "29"
  }
}
```

#### Changing the bulk import class

Sleeper contains three different Spark algorithms for performing the bulk import. There are two approaches that use
Dataframes and one that uses RDDs (recall that algorithms expressed using Spark's Dataframe API are normally more
efficient than RDD-based algorithms).

The default algorithm is the `BulkImportDataframeLocalSortDriver`. This partitions the data according to Sleeper's leaf
partitions and then sorts within partitions. In general this is more efficient than the `BulkImportJobDataframeDriver`
algorithm which globally sorts the data before ingesting data. This will result in more files than there are Sleeper
partitions. However, if the number of Sleeper leaf partitions is small then this allows more parallelism than
the `BulkImportDataframeLocalSortDriver` approach.

The RDD-based approach uses the `repartitionAndSortWithinPartitions` method on an RDD formed from the input data. This
is generally significantly less efficient than the Dataframe-based methods.

To change the algorithm used on a per-job basis, set the `className` field on the JSON for the job, e.g.:

```JSON
{
  "className": "sleeper.bulkimport.runner.rdd.BulkImportJobRDDDriver"
}
```

The instance property `sleeper.bulk.import.class.name` can be used to set the default algorithm.

#### Bulk import on EKS

The `EksBulkImportStack` option requires the bulk import Docker image to be pushed to ECR - see the instructions in the
[deployment guide](../deployment-guide.md).

It's also important to configure a role to be mapped into EKS to administer the cluster. This will allow you to connect
with `kubectl` and access Kubernetes resources in the AWS console. Look in AWS IAM, and choose a role that gets assigned
to your user, or users with administrator access. This may be in the form AWSReservedSSO_AdministratorAccess_abc123 if
you log in with SSO, or OrganizationAccountAccessRole if you log in with an AWS Organisation.

Any roles you want to give access to the cluster should be set in an instance property like this:

```properties
sleeper.bulk.import.eks.cluster.admin.roles=AWSReservedSSO_AdministratorAccess_abc123,OrganizationAccountAccessRole
```

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
    "spark.executor.memoryOverhead": "1g",
    "spark.driver.cores": "1",
    "spark.executor.cores": "1"
  }
}
```

You can change the memory settings and number of executors. The settings shown are the default ones so will be
included even if you don't ask for them. It's important that your driver and executor memory and CPU settings are
compatible
with [AWS Fargate's supported values](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html)
otherwise the job will fail to start. The total memory for the spark driver or executor is calculated by adding
the `spark.[driver/executor].memory` and `spark.[driver/executor].memoryOverhead` The memory overhead should be
around 10% of the executor memory. Otherwise you start to run into memory issues on Kubernetes and your nodes
will start being killed.

The Spark job will be run with a service account which has permissions to read from the bulk import bucket, and write to
all the Sleeper tables in the instance so there should be no issues with permissions.

The bulk import job will go through some initial validation, and if successful will be transformed and submitted to an
AWS Step Functions State Machine. If the job fails validation, or for some reason is unable to be submitted to the State
Machine, a CloudWatch alarm will trigger and an email will be sent to the address specified in `sleeper.errors.email`.

When the job makes it to the State Machine, it will run the job synchronously and watch its status. If the job is
successful, the job will be torn down automatically. If unsuccessful or the job doesn't submit, a notification will be
sent to the errors email.

##### Debugging and UI access

While a Spark job is running you'll be able to monitor it with the Spark UI. You can also monitor jobs in the AWS
console for EKS and Step Functions.

To use the Spark UI, or access the EKS cluster, ensure you've configured the instance
property `sleeper.bulk.import.eks.cluster.admin.roles` as explained earlier. Without this, you'll need to use Step
Functions to monitor your jobs as you won't have access to Kubernetes.

Once the `EksBulkImportStack` is deployed, there should be an EKS cluster with a name
like `sleeper-<my instance ID>-eksBulkImportCluster`. You can find the name of the cluster in the EKS console, or in
CloudFormation in the nested stack linked from the root stack at `BulkImportEKS.NestedStack`, in a resource with a
logical ID like `BulkImportEKS.EksBulkImportCluster.Resource.Resource.EksBulkImportClusterABCD1234`.

To access the Spark UI, you'll need to install `kubectl`, a command line utility for Kubernetes. Once you've done that,
run this command in a terminal: `aws eks update-kubeconfig --name <cluster name>`

You should now be able to inspect logs, list pods and connect remotely to the Spark UI. The driver pods all use the job
ID as the name. If you don't set this manually, it will be a random UUID.

```bash
instance_id=abc1234

# This shortcut means we don't have to add -n <the namespace> to all our commands
kubectl config set-context --current --namespace sleeper-${instance_id}-eks-bulk-import

# Inspect the logs (add -f to follow them)
kubectl logs pods/my-job-name

# Forward connections to the spark UI (type localhost:4040 into your browser to bring it up)
kubectl port-forward my-job-name 4040:4040
```
