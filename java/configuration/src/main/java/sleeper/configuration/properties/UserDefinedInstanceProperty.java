/*
 * Copyright 2022-2023 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.configuration.properties;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import sleeper.configuration.Utils;

import java.util.Objects;

import static sleeper.configuration.properties.UserDefinedInstancePropertyImpl.named;

/**
 * Sleeper properties set by the user. All non-mandatory properties should be accompanied by a default value and should
 * have a validation predicate for determining if the value a user has provided is valid. By default the predicate
 * always returns true indicating the property is valid.
 */
// Suppress as this class will always be referenced before impl class, so initialization behaviour will be deterministic
@SuppressFBWarnings("IC_SUPERCLASS_USES_SUBCLASS_DURING_INITIALIZATION")
public interface UserDefinedInstanceProperty extends InstanceProperty {
    // Tables
    UserDefinedInstanceProperty TABLE_PROPERTIES = named("sleeper.table.properties")
            .description("A comma separated list of paths containing the table properties files. These can either be paths to\n" +
                    "the properties files themselves or paths to directories which contain the table properties.")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(PropertyGroup.COMMON).build();
    // Common
    UserDefinedInstanceProperty ID = named("sleeper.id")
            .description("A string to uniquely identify this deployment. This should be no longer than 20 chars.\n" +
                    "It should be globally unique as it will be used to name AWS resources such as S3 buckets.")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty JARS_BUCKET = named("sleeper.jars.bucket")
            .description("The S3 bucket containing the jar files of the Sleeper components.")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty USER_JARS = named("sleeper.userjars")
            .description("A comma-separated list of the jars containing application specific iterator code.\n" +
                    "These jars are assumed to be in the bucket given by sleeper.jars.bucket, e.g. if that\n" +
                    "bucket contains two iterator jars called iterator1.jar and iterator2.jar then the\n" +
                    "property should be sleeper.userjars=iterator1.jar,iterator2.jar")
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty TAGS_FILE = named("sleeper.tags.file")
            .description("A file of key-value tags. These will be added to all the resources in this deployment.")
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty TAGS = named("sleeper.tags")
            .description("A list of tags for the project")
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty STACK_TAG_NAME = named("sleeper.stack.tag.name")
            .description("A name for a tag to identify the stack that deployed a resource. This will be set for all AWS resources, to the ID of\n" +
                    "the CDK stack that they are deployed under. This can be used to organise the cost explorer for billing.")
            .defaultValue("DeploymentStack")
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty RETAIN_INFRA_AFTER_DESTROY = named("sleeper.retain.infra.after.destroy")
            .description("Whether to keep the sleeper table bucket, Dynamo tables, query results bucket, etc., \n" +
                    "when the instance is destroyed")
            .defaultValue("true")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty OPTIONAL_STACKS = named("sleeper.optional.stacks")
            .description("The optional stacks to deploy")
            .defaultValue("CompactionStack,GarbageCollectorStack,IngestStack,PartitionSplittingStack,QueryStack,AthenaStack,EmrBulkImportStack,DashboardStack")
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty ACCOUNT = named("sleeper.account")
            .description("The AWS account number. This is the AWS account that the instance will be deployed to")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty REGION = named("sleeper.region")
            .description("The AWS region to deploy to")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty VERSION = named("sleeper.version")
            .description("The version of Sleeper to use. This property is used to identify the correct jars in the S3JarsBucket and to\n" +
                    "select the correct tag in the ECR repositories.")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty VPC_ID = named("sleeper.vpc")
            .description("The id of the VPC to deploy to")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty VPC_ENDPOINT_CHECK = named("sleeper.vpc.endpoint.check")
            .description("Whether to check that the VPC that the instance is deployed to has an S3 endpoint")
            .defaultValue("true")
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty SUBNET = named("sleeper.subnet")
            .description("The subnet to deploy ECS tasks to")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty FILE_SYSTEM = named("sleeper.filesystem")
            .description("The subnet to deploy ECS tasks to")
            .defaultValue("s3a://")
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty EMAIL_ADDRESS_FOR_ERROR_NOTIFICATION = named("sleeper.errors.email")
            .description("An email address used by the TopicStack to publish SNS notifications of errors")
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS = named("sleeper.queue.visibility.timeout.seconds")
            .description("The visibility timeout on the queues used in compactions, partition splitting, etc")
            .defaultValue("900")
            .validationPredicate(Utils::isValidLambdaTimeout)
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty LOG_RETENTION_IN_DAYS = named("sleeper.log.retention.days")
            .description("The length of time in days that CloudWatch logs are retained")
            .defaultValue("30")
            .validationPredicate(Utils::isValidLogRetention)
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty MAXIMUM_CONNECTIONS_TO_S3 = named("sleeper.s3.max-connections")
            .description("Used to set the value of fs.s3a.connection.maximum on the Hadoop configuration")
            .defaultValue("25")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty FARGATE_VERSION = named("sleeper.fargate.version")
            .description("The version of Fargate to use")
            .defaultValue("1.4.0")
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty TASK_RUNNER_LAMBDA_MEMORY_IN_MB = named("sleeper.task.runner.memory")
            .description("The amount of memory for the lambda that creates ECS tasks to execute compaction and ingest jobs")
            .defaultValue("1024")
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS = named("sleeper.task.runner.timeout.seconds")
            .description("The timeout in seconds for the lambda that creates ECS tasks to execute compaction jobs and ingest jobs\n" +
                    "This must be >0 and <= 900.")
            .defaultValue("900")
            .validationPredicate(Utils::isValidLambdaTimeout)
            .propertyGroup(PropertyGroup.COMMON).build();
    UserDefinedInstanceProperty METRICS_NAMESPACE = named("sleeper.metrics.namespace")
            .description("The namespaces for the metrics used in the metrics stack")
            .defaultValue("Sleeper")
            .validationPredicate(Utils::isNonNullNonEmptyString)
            .propertyGroup(PropertyGroup.COMMON).build();

    // Ingest
    UserDefinedInstanceProperty ECR_INGEST_REPO = named("sleeper.ingest.repo")
            .description("The name of the ECR repository for the ingest container. The Docker image from the ingest module should have been\n" +
                    "# uploaded to an ECR repository of this name in this account.")
            .propertyGroup(PropertyGroup.INGEST).build();
    UserDefinedInstanceProperty MAXIMUM_CONCURRENT_INGEST_TASKS = named("sleeper.ingest.max.concurrent.tasks")
            .description("The maximum number of concurrent ECS tasks to run")
            .defaultValue("200")
            .propertyGroup(PropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_TASK_CREATION_PERIOD_IN_MINUTES = named("sleeper.ingest.task.creation.period.minutes")
            .description("The frequency in minutes with which an EventBridge rule runs to trigger a lambda that, if necessary, runs more ECS\n" +
                    "tasks to perform ingest jobs.")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(PropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_KEEP_ALIVE_PERIOD_IN_SECONDS = named("sleeper.ingest.keepalive.period.seconds")
            .description("The frequency, in seconds, with which change message visibility requests are sent to extend the\n" +
                    "visibility of messages on the ingest queue so that they are not processed by other processes.\n" +
                    "This should be less than the value of sleeper.queue.visibility.timeout.seconds.")
            .defaultValue("300")
            .propertyGroup(PropertyGroup.INGEST).build();
    UserDefinedInstanceProperty S3A_INPUT_FADVISE = named("sleeper.ingest.fs.s3a.experimental.input.fadvise")
            .description("This sets the value of fs.s3a.experimental.input.fadvise on the Hadoop configuration used to read and write\n" +
                    "files to and from S3 in ingest jobs. Changing this value allows you to fine-tune how files are read. Possible\n" +
                    "values are \"normal\", \"sequential\" and \"random\". More information is available here:\n" +
                    "https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/performance.html#fadvise")
            .defaultValue("sequential")
            .validationPredicate(Utils::isValidFadvise)
            .propertyGroup(PropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_TASK_CPU = named("sleeper.ingest.task.cpu")
            .description("The amount of CPU used by Fargate tasks that perform ingest jobs")
            .defaultValue("2048")
            .propertyGroup(PropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_TASK_MEMORY = named("sleeper.ingest.task.memory")
            .description("The amount of memory used by Fargate tasks that perform ingest jobs")
            .defaultValue("4096")
            .propertyGroup(PropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS = named("sleeper.ingest.partition.refresh.period")
            .description("The frequeney in seconds with which ingest tasks refresh their view of the partitions\n" +
                    "(NB Refreshes only happen once a batch of data has been written so this is a lower bound\n" +
                    "on the refresh frequency.)")
            .defaultValue("120")
            .propertyGroup(PropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_SOURCE_BUCKET = named("sleeper.ingest.source.bucket")
            .description("The name of a bucket that contains files to be ingested via ingest jobs. This bucket should already\n" +
                    "exist, i.e. it will not be created as part of the cdk deployment of this instance of Sleeper. The ingest\n" +
                    "and bulk import stacks will be given read access to this bucket so that they can consume data from it.")
            .propertyGroup(PropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_RECORD_BATCH_TYPE = named("sleeper.ingest.record.batch.type")
            .description("The way in which records are held in memory before they are written to a local store.\n" +
                    "Valid values are 'arraylist' and 'arrow'.\n" +
                    "The arraylist method is simpler, but it is slower and requires careful tuning of the number of records in each batch.\n" +
                    "Note that the arrow approach does not currently support schemas containing lists or maps.")
            .defaultValue("arraylist")
            .propertyGroup(PropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_PARTITION_FILE_WRITER_TYPE = named("sleeper.ingest.partition.file.writer.type")
            .description("The way in which partition files are written to the main Sleeper store\n" +
                    "Valid values are 'direct' (which writes using the s3a Hadoop file system) and 'async' (which writes locally and then\n" +
                    "copies the completed Parquet file asynchronously into S3).\n" +
                    "The direct method is simpler but the async method should provide better performance when the number of partitions\n" +
                    "is large.")
            .defaultValue("direct")
            .propertyGroup(PropertyGroup.INGEST).build();

    // ArrayList ingest
    UserDefinedInstanceProperty MAX_RECORDS_TO_WRITE_LOCALLY = named("sleeper.ingest.max.local.records")
            .description("The maximum number of records written to local file in an ingest job. (Records are written in sorted order to local\n" +
                    "disk before being uploaded to S3. Increasing this value increases the amount of time before data is visible in the\n" +
                    "system, but increases the number of records written to S3 in a batch, therefore reducing costs.)\n" +
                    "(arraylist-based ingest only)")
            .defaultValue("100000000")
            .propertyGroup(PropertyGroup.INGEST).build();
    UserDefinedInstanceProperty MAX_IN_MEMORY_BATCH_SIZE = named("sleeper.ingest.memory.max.batch.size")
            .description("The maximum number of records to read into memory in an ingest job. (Up to sleeper.ingest.memory.max.batch.size\n" +
                    "records are read into memory before being sorted and written to disk. This process is repeated until\n" +
                    "sleeper.ingest.max.local.records records have been written to local files. Then the sorted files and merged and\n" +
                    "the data is written to sorted files in S3.)\n" +
                    "(arraylist-based ingest only)")
            .defaultValue("1000000")

            .propertyGroup(PropertyGroup.INGEST).build();
    // Arrow ingest
    UserDefinedInstanceProperty ARROW_INGEST_WORKING_BUFFER_BYTES = named("sleeper.ingest.arrow.working.buffer.bytes")
            .description("The number of bytes to allocate to the Arrow working buffer. This buffer is used for sorting and other sundry\n" +
                    "activities.\n" +
                    "Note that this is off-heap memory, which is in addition to the memory assigned to the JVM.\n" +
                    "(arrow-based ingest only) [256MB]")
            .defaultValue("268435456")
            .propertyGroup(PropertyGroup.INGEST).build();
    UserDefinedInstanceProperty ARROW_INGEST_BATCH_BUFFER_BYTES = named("sleeper.ingest.arrow.batch.buffer.bytes")
            .description("The number of bytes to allocate to the Arrow batch buffer, which is used to hold the records before they are\n" +
                    "written to local disk. A larger value means that the local disk holds fewer, larger files, which are more efficient\n" +
                    "to merge together during an upload to S3. Larger values may require a larger working buffer.\n" +
                    "Note that this is off-heap memory, which is in addition to the memory assigned to the JVM.\n" +
                    "(arrow-based ingest only) [1GB]")
            .defaultValue("1073741824")
            .propertyGroup(PropertyGroup.INGEST).build();
    UserDefinedInstanceProperty ARROW_INGEST_MAX_LOCAL_STORE_BYTES = named("sleeper.ingest.arrow.max.local.store.bytes")
            .description("The maximum number of bytes to store on the local disk before uploading to the main Sleeper store. A larger value\n" +
                    "reduces the number of S3 PUTs that are required to upload thle data to S3 and results in fewer files per partition.\n" +
                    "(arrow-based ingest only) [2GB]")
            .defaultValue("2147483648")
            .propertyGroup(PropertyGroup.INGEST).build();
    UserDefinedInstanceProperty ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS = named("sleeper.ingest.arrow.max.single.write.to.file.records")
            .description("The number of records to write at once into an Arrow file in the local store. A single Arrow file contains many of\n" +
                    "these micro-batches and so this parameter does not significantly affect the final size of the Arrow file.\n" +
                    "Larger values may require a larger working buffer.\n" +
                    "(arrow-based ingest only) [1K]")
            .defaultValue("1024")
            .propertyGroup(PropertyGroup.INGEST).build();

    // Async ingest partition file writer
    UserDefinedInstanceProperty ASYNC_INGEST_CLIENT_TYPE = named("sleeper.ingest.async.client.type")
            .description("The implementation of the async S3 client to use for upload during ingest.\n" +
                    "Valid values are 'java' or 'crt'. This determines the implementation of S3AsyncClient that gets used.\n" +
                    "With 'java' it makes a single PutObject request for each file.\n" +
                    "With 'crt' it uses the AWS Common Runtime (CRT) to make multipart uploads.\n" +
                    "Note that the CRT option is recommended. Using the Java option may cause failures if any file is >5GB in size, and\n" +
                    "will lead to the following warning:\n" +
                    "\"The provided S3AsyncClient is not an instance of S3CrtAsyncClient, and thus multipart upload/download feature is not\n" +
                    "enabled and resumable file upload is not supported. To benefit from maximum throughput, consider using\n" +
                    "S3AsyncClient.crtBuilder().build() instead.\"\n" +
                    "(async partition file writer only)")
            .defaultValue("crt")
            .propertyGroup(PropertyGroup.INGEST).build();
    UserDefinedInstanceProperty ASYNC_INGEST_CRT_PART_SIZE_BYTES = named("sleeper.ingest.async.crt.part.size.bytes")
            .description("The part size in bytes to use for multipart uploads.\n" +
                    "(CRT async ingest only) [128MB]")
            .defaultValue("134217728") // 128M
            .validationPredicate(Utils::isPositiveLong)
            .propertyGroup(PropertyGroup.INGEST).build();
    UserDefinedInstanceProperty ASYNC_INGEST_CRT_TARGET_THROUGHPUT_GBPS = named("sleeper.ingest.async.crt.target.throughput.gbps")
            .description("The target throughput for multipart uploads, in GB/s. Determines how many parts should be uploaded simultaneously.\n" +
                    "(CRT async ingest only)")
            .defaultValue("10")
            .validationPredicate(Utils::isPositiveDouble)

            .propertyGroup(PropertyGroup.INGEST).build();
    // Status Store
    UserDefinedInstanceProperty INGEST_STATUS_STORE_ENABLED = named("sleeper.ingest.status.store.enabled")
            .description("Flag to enable/disable storage of tracking information for ingest jobs and tasks")
            .defaultValue("true")
            .propertyGroup(PropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_JOB_STATUS_TTL_IN_SECONDS = named("sleeper.ingest.job.status.ttl")
            .description("The time to live in seconds for ingest job updates in the status store. Default is 1 week")
            .defaultValue("604800") // Default is 1 week
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(PropertyGroup.INGEST).build();

    UserDefinedInstanceProperty INGEST_TASK_STATUS_TTL_IN_SECONDS = named("sleeper.ingest.task.status.ttl")
            .description("The time to live in seconds for ingest task updates in the status store. Default is 1 week")
            .defaultValue("604800") // Default is 1 week
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(PropertyGroup.INGEST).build();

    // Bulk Import - properties that are applicable to all bulk import platforms
    UserDefinedInstanceProperty BULK_IMPORT_CLASS_NAME = named("sleeper.bulk.import.class.name")
            .description("The class to use to perform the bulk import. The default value below uses Spark Dataframes. There is an\n" +
                    "alternative option that uses RDDs (sleeper.bulkimport.job.runner.rdd.BulkImportJobRDDRunner).")
            .defaultValue("sleeper.bulkimport.job.runner.dataframelocalsort.BulkImportDataframeLocalSortRunner")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_SHUFFLE_MAPSTATUS_COMPRESSION_CODEC = named("sleeper.bulk.import.emr.spark.shuffle.mapStatus.compression.codec")
            .description("The compression codec for map status results." +
                    "Stops \"Decompression error: Version not supported\" errors - only a value of \"lz4\" has been tested. " +
                    "This is used to set the value of spark.shuffle.mapStatus.compression.codec on the Spark configuration.")
            .defaultValue("lz4")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_SPECULATION = named("sleeper.bulk.import.emr.spark.speculation")
            .description("This is used to set the value of spark.speculation on the Spark configuration. " +
                    "If true then speculative execution of tasks will be performed. Used to set spark.speculation.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_SPECULATION_QUANTILE = named("sleeper.bulk.import.spark.speculation.quantile")
            .description("This is used to set the value of spark.speculation.quantile on the Spark configuration." +
                    "The fraction of tasks which must be complete before speculation is enabled for a particular stage.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("0.75")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();

    // Bulk import using EMR - these properties are used by both the persistent
    // and non-persistent EMR stacks
    UserDefinedInstanceProperty BULK_IMPORT_EMR_EC2_KEYPAIR_NAME = named("sleeper.bulk.import.emr.keypair.name")
            .description("(Non-persistent or persistent EMR mode only) An EC2 keypair to use for the EC2 instances. Specifying this will allow you to SSH to the nodes\n" +
                    "in the cluster while it's running.")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP = named("sleeper.bulk.import.emr.master.additional.security.group")
            .description("(Non-persistent or persistent EMR mode only) Specifying this security group causes the group\n" +
                    "to be added to the EMR master's list of security groups")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    //  - The following properties depend on the instance type and number of instances - they have been chosen
    //          based on the default settings for the EMR and persistent EMR clusters (these are currently the
    //          same which allows the following properties to be used across both types):
    //      - Theses are based on this blog
    //      https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/
    //      - Our default core/task instance type is m5.4xlarge. These have 64GB of RAM and 16 vCPU. The amount of
    //          usable RAM is 56GB.
    //      - The recommended value of spark.executor.cores is 5, irrespective of the number of servers or their specs.
    //      - Number of executors per instance = (number of vCPU per instance - 1) / spark.executors.cores = (16 - 1) / 5 = 3
    //      - Total executor memory = total RAM per instance / number of executors per instance = 56 / 3 = 18 (rounded down)
    //      - Assign 90% of the total executor memory to the executor and 10% to the overhead
    //      - spark.executor.memory = 0.9 * 18 = 16GB (memory must be an integer)
    //      - spark.yarn.executor.memoryOverhead = 0.1 * 18 = 2GB
    //      - spark.driver.memory = spark.executor.memory
    //      - spark.driver.cores = spark.executor.core
    //      - spark.executor.instances = (number of executors per instance * number of core&task instances) - 1 = 3 * 10 - 1 = 29
    //      - spark.default.parallelism = spark.executor.instances * spark.executor.cores * 2 = 29 * 5 * 2 = 290
    //      - spark.sql.shuffle.partitions = spark.default.parallelism
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_EXECUTOR_MEMORY = named("sleeper.bulk.import.emr.spark.executor.memory")
            .description("The amount of memory allocated to a Spark executor. Used to set spark.executor.memory.")
            .defaultValue("16g")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_DRIVER_MEMORY = named("sleeper.bulk.import.emr.spark.driver.memory")
            .description("The amount of memory allocated to the Spark driver. Used to set spark.driver.memory")
            .defaultValue(BULK_IMPORT_EMR_SPARK_EXECUTOR_MEMORY.getDefaultValue())
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_EXECUTOR_INSTANCES = named("sleeper.bulk.import.emr.spark.executor.instances")
            .description("The number of executors. Used to set spark.executor.instances")
            .defaultValue("29")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD = named("sleeper.bulk.import.emr.spark.yarn.executor.memory.overhead")
            .description("The memory overhead for the driver. Used to set spark.yarn.driver.memoryOverhead")
            .defaultValue("2g")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_YARN_DRIVER_MEMORY_OVERHEAD = named("sleeper.bulk.import.emr.spark.yarn.driver.memory.overhead")
            .description("The memory overhead for an executor. Used to set spark.yarn.executor.memoryOverhead")
            .defaultValue(BULK_IMPORT_EMR_SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD.getDefaultValue())
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_DEFAULT_PARALLELISM = named("sleeper.bulk.import.emr.spark.default.parallelism")
            .description("The default parallelism for Spark job. Used to set spark.default.parallelism")
            .defaultValue("290")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_SQL_SHUFFLE_PARTITIONS = named("sleeper.bulk.import.emr.spark.sql.shuffle.partitions")
            .description("The number of partitions used in a Spark SQL/dataframe shuffle operation. Used to set spark.sql.shuffle.partitions")
            .defaultValue(BULK_IMPORT_EMR_SPARK_DEFAULT_PARALLELISM.getDefaultValue())
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    //  - Properties that are independent of the instance type and number of instances:
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_EXECUTOR_CORES = named("sleeper.bulk.import.emr.spark.executor.cores")
            .description("(Non-persistent or persistent EMR mode only) The number of cores used by an executor\n" +
                    "Used to set spark.executor.cores")
            .defaultValue("5")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_DRIVER_CORES = named("sleeper.bulk.import.emr.spark.driver.cores")
            .description("(Non-persistent or persistent EMR mode only) The number of cores used by the driver\n" +
                    "Used to set spark.driver.cores")
            .defaultValue(BULK_IMPORT_EMR_SPARK_EXECUTOR_CORES.getDefaultValue())
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_NETWORK_TIMEOUT = named("sleeper.bulk.import.emr.spark.network.timeout")
            .description("(Non-persistent or persistent EMR mode only) The default timeout for network interactions in Spark\n" +
                    "Used to set spark.network.timeout")
            .defaultValue("800s")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_EXECUTOR_HEARTBEAT_INTERVAL = named("sleeper.bulk.import.emr.spark.executor.heartbeat.interval")
            .description("(Non-persistent or persistent EMR mode only) The interval between heartbeats from executors to the driver\n" +
                    "Used to set spark.executor.heartbeatInterval")
            .defaultValue("60s")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_DYNAMIC_ALLOCATION_ENABLED = named("sleeper.bulk.import.emr.spark.dynamic.allocation.enabled")
            .description("(Non-persistent or persistent EMR mode only) Whether Spark should use dynamic allocation to scale resources up and down\n" +
                    "Used to set spark.dynamicAllocation.enabled")
            .defaultValue("false")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_MEMORY_FRACTION = named("sleeper.bulk.import.emr.spark.memory.fraction")
            .description("(Non-persistent or persistent EMR mode only) The fraction of heap space used for execution and storage\n" +
                    "Used to set spark.memory.fraction")
            .defaultValue("0.80")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_MEMORY_STORAGE_FRACTION = named("sleeper.bulk.import.emr.spark.memory.storage.fraction")
            .description("(Non-persistent or persistent EMR mode only) The amount of storage memory immune to eviction,\n" +
                    "expressed as a fraction of the heap space used for execution and storage. Used to set spark.memory.storageFraction.")
            .defaultValue("0.30")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS = named("sleeper.bulk.import.emr.spark.executor.extra.java.options")
            .description("(Non-persistent or persistent EMR mode only) JVM options passed to the executors\n" +
                    "Used to set spark.executor.extraJavaOptions")
            .defaultValue("-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_DRIVER_EXTRA_JAVA_OPTIONS = named("sleeper.bulk.import.emr.spark.driver.extra.java.options")
            .description("(Non-persistent or persistent EMR mode only) JVM options passed to the driver.\n" +
                    "Used to set spark.driver.extraJavaOptions.")
            .defaultValue(BULK_IMPORT_EMR_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS.getDefaultValue())
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_YARN_SCHEDULER_REPORTER_THREAD_MAX_FAILURES = named("sleeper.bulk.import.emr.spark.yarn.scheduler.reporter.thread.max.failures")
            .description("(Non-persistent or persistent EMR mode only) The maximum number of executor failures before YARN can fail the application.\n" +
                    "Used to set spark.yarn.scheduler.reporterThread.maxFailures.")
            .defaultValue("5")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_STORAGE_LEVEL = named("sleeper.bulk.import.emr.spark.storage.level")
            .description("(Non-persistent or persistent EMR mode only) The storage to use for temporary caching.\n" +
                    "Used to set spark.storage.level.")
            .defaultValue("MEMORY_AND_DISK_SER")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_RDD_COMPRESS = named("sleeper.bulk.import.emr.spark.rdd.compress")
            .description("(Non-persistent or persistent EMR mode only) Whether to compress serialized RDD partitions.\n" +
                    "Used to set spark.rdd.compress.")
            .defaultValue("true")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_SHUFFLE_COMPRESS = named("sleeper.bulk.import.emr.spark.shuffle.compress")
            .description("(Non-persistent or persistent EMR mode only) Whether to compress map output files.\n" +
                    "Used to set spark.shuffle.compress.")
            .defaultValue("true")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_SHUFFLE_SPILL_COMPRESS = named("sleeper.bulk.import.emr.spark.shuffle.spill.compress")
            .description("(Non-persistent or persistent EMR mode only) Whether to compress data spilled during shuffles.\n" +
                    "Used to set spark.shuffle.spill.compress.")
            .defaultValue("true")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_EBS_VOLUME_SIZE_IN_GB = named("sleeper.bulk.import.emr.ebs.volume.size.gb")
            .description("(Non-persistent or persistent EMR mode only) The size of the EBS volume in gibibytes (GiB).\n" +
                    "This can be a number from 10 to 1024.")
            .defaultValue("256")
            .validationPredicate(Utils::isValidEbsSize)
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_EBS_VOLUME_TYPE = named("sleeper.bulk.import.emr.ebs.volume.type")
            .description("(Non-persistent or persistent EMR mode only) The type of the EBS volume.\n" +
                    "Valid values are 'gp2', 'gp3', 'io1', 'io2'.")
            .defaultValue("gp2")
            .validationPredicate(Utils::isValidEbsVolumeType)
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_EBS_VOLUMES_PER_INSTANCE = named("sleeper.bulk.import.emr.ebs.volumes.per.instance")
            .description("(Non-persistent or persistent EMR mode only) The number of EBS volumes per instance.\n" +
                    "This can be a number from 1 to 25.")
            .defaultValue("4").validationPredicate(s -> Utils.isPositiveIntLtEqValue(s, 25))
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();

    // Bulk import using the non-persistent EMR approach
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL = named("sleeper.default.bulk.import.emr.release.label")
            .description("(Non-persistent EMR mode only) The default EMR release label to be used when creating an EMR cluster for bulk importing data\n" +
                    "using Spark running on EMR. This default can be overridden by a table property or by a property in the\n" +
                    "bulk import job specification.")
            .defaultValue("emr-6.9.0")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE = named("sleeper.default.bulk.import.emr.master.instance.type")
            .description("(Non-persistent EMR mode only) The default EC2 instance type to be used for the master node of the EMR cluster.\n" +
                    "This default can be overridden by a table property or by a property in the bulk import job specification.")
            .defaultValue("m5.xlarge")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE = named("sleeper.default.bulk.import.emr.executor.market.type")
            .defaultValue("SPOT").validationPredicate(s -> ("SPOT".equals(s) || "ON_DEMAND".equals(s)))
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE = named("sleeper.default.bulk.import.emr.executor.instance.type")
            .description("(Non-persistent EMR mode only) The default EC2 instance type to be used for the executor nodes of the EMR cluster.\n" +
                    "This default can be overridden by a table property or by a property in the bulk import job specification")
            .defaultValue("m5.4xlarge")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS = named("sleeper.default.bulk.import.emr.executor.initial.instances")
            .description("(Non-persistent EMR mode only) The default initial number of EC2 instances to be used as executors in the EMR cluster.\n" +
                    "This default can be overridden by a table property or by a property in the bulk import job specification.")
            .defaultValue("2")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS = named("sleeper.default.bulk.import.emr.executor.max.instances")
            .description("(Non-persistent EMR mode only) The default maximum number of EC2 instances to be used as executors in the EMR cluster.\n" +
                    "This default can be overridden by a table property or by a property in the bulk import job specification.")
            .defaultValue("10")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();

    // Bulk import using a persistent EMR cluster
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_RELEASE_LABEL = named("sleeper.bulk.import.persistent.emr.release.label")
            .description("(Persistent EMR mode only) The EMR release used to create the persistent EMR cluster.")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL.getDefaultValue())
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MASTER_INSTANCE_TYPE = named("sleeper.bulk.import.persistent.emr.master.instance.type")
            .description("(Persistent EMR mode only) The EC2 instance type used for the master of the persistent EMR cluster.")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE.getDefaultValue())
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_INSTANCE_TYPE = named("sleeper.bulk.import.persistent.emr.core.instance.type")
            .description("(Persistent EMR mode only) The EC2 instance type used for the executor nodes of the persistent EMR cluster.")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE.getDefaultValue())
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING = named("sleeper.bulk.import.persistent.emr.use.managed.scaling")
            .description("(Persistent EMR mode only) Whether the persistent EMR cluster should use managed scaling or not.")
            .defaultValue("true")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MIN_NUMBER_OF_INSTANCES = named("sleeper.bulk.import.persistent.emr.min.instances")
            .description("(Persistent EMR mode only) The minimum number of instances in the persistent EMR cluster.\n" +
                    "If managed scaling is not used then the cluster will be of fixed size, with number of instances equal to this value.")
            .defaultValue("1")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MAX_NUMBER_OF_INSTANCES = named("sleeper.bulk.import.persistent.emr.max.instances")
            .description("(Persistent EMR mode only) The maximum number of instances in the persistent EMR cluster.\n" +
                    "This value is only used if managed scaling is not used.")
            .defaultValue("10")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_STEP_CONCURRENCY_LEVEL = named("sleeper.bulk.import.persistent.emr.step.concurrency.level")
            .description("(Persistent EMR mode only) This controls the number of EMR steps that can run concurrently.")
            .defaultValue("2")
            .propertyGroup(PropertyGroup.BULK_IMPORT).build();

    // Bulk import using EKS
    UserDefinedInstanceProperty BULK_IMPORT_REPO = named("sleeper.bulk.import.eks.repo")
            .description("(EKS mode only) The name of the ECS repository where the Docker image for the bulk import container is stored.")
            .build();

    // Partition splitting
    UserDefinedInstanceProperty PARTITION_SPLITTING_PERIOD_IN_MINUTES = named("sleeper.partition.splitting.period.minutes")
            .description("The frequency in minutes with which the lambda that finds partitions that need splitting runs.")
            .defaultValue("2")
            .propertyGroup(PropertyGroup.PARTITION_SPLITTING).build();
    UserDefinedInstanceProperty MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB = named("sleeper.partition.splitting.files.maximum")
            .description("When a partition needs splitting, a partition splitting job is created. This reads in the sketch files\n" +
                    "associated to the files in the partition in order to identify the median. This parameter controls the\n" +
                    "maximum number of files that are read in.")
            .defaultValue("50")
            .propertyGroup(PropertyGroup.PARTITION_SPLITTING).build();
    UserDefinedInstanceProperty FIND_PARTITIONS_TO_SPLIT_LAMBDA_MEMORY_IN_MB = named("sleeper.partition.splitting.finder.memory")
            .description("The amount of memory in MB for the lambda function used to identify partitions that need to be split.")
            .defaultValue("2048")
            .propertyGroup(PropertyGroup.PARTITION_SPLITTING).build();
    UserDefinedInstanceProperty FIND_PARTITIONS_TO_SPLIT_TIMEOUT_IN_SECONDS = named("sleeper.partition.splitting.finder.timeout.seconds")
            .description("The timeout in seconds for the lambda function used to identify partitions that need to be split.")
            .defaultValue("900")
            .propertyGroup(PropertyGroup.PARTITION_SPLITTING).build();
    UserDefinedInstanceProperty SPLIT_PARTITIONS_LAMBDA_MEMORY_IN_MB = named("sleeper.partition.splitting.memory")
            .description("The memory for the lambda function used to split partitions.")
            .defaultValue("2048")
            .propertyGroup(PropertyGroup.PARTITION_SPLITTING).build();
    UserDefinedInstanceProperty SPLIT_PARTITIONS_TIMEOUT_IN_SECONDS = named("sleeper.partition.splitting.timeout.seconds")
            .description("The timeout in seconds for the lambda function used to split partitions.")
            .defaultValue("900")
            .propertyGroup(PropertyGroup.PARTITION_SPLITTING).build();
    UserDefinedInstanceProperty DEFAULT_PARTITION_SPLIT_THRESHOLD = named("sleeper.default.partition.splitting.threshold")
            .description("This is the default value of the partition splitting threshold. Partitions with more than the following\n" +
                    "number of records in will be split. This value can be overridden on a per-table basis.")
            .defaultValue("1000000000")
            .propertyGroup(PropertyGroup.PARTITION_SPLITTING).build();

    // Garbage collection
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_PERIOD_IN_MINUTES = named("sleeper.gc.period.minutes")
            .description("The frequency in minutes with which the garbage collector lambda is run.")
            .defaultValue("15")
            .propertyGroup(PropertyGroup.GARBAGE_COLLECTOR).build();
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_LAMBDA_MEMORY_IN_MB = named("sleeper.gc.memory")
            .description("The memory in MB for the lambda function used to perform garbage collection.")
            .defaultValue("1024")
            .propertyGroup(PropertyGroup.GARBAGE_COLLECTOR).build();
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_BATCH_SIZE = named("sleeper.gc.batch.size")
            .description("The size of the batch of files ready for garbage collection requested from the State Store.")
            .defaultValue("2000")
            .propertyGroup(PropertyGroup.GARBAGE_COLLECTOR).build();
    UserDefinedInstanceProperty DEFAULT_GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION = named("sleeper.default.gc.delay.seconds")
            .description("A file will not be deleted until this number of seconds have passed after it has been marked as ready for\n" +
                    "garbage collection. The reason for not deleting files immediately after they have been marked as ready for\n" +
                    "garbage collection is that they may still be in use by queries. This property can be overridden on a per-table\n" +
                    "basis.")
            .defaultValue("600")
            .propertyGroup(PropertyGroup.GARBAGE_COLLECTOR).build();

    // Compaction
    UserDefinedInstanceProperty ECR_COMPACTION_REPO = named("sleeper.compaction.repo")
            .description("The name of the repository for the compaction container. The Docker image from the compaction-job-execution module\n" +
                    "should have been uploaded to an ECR repository of this name in this account.")
            .propertyGroup(PropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS = named("sleeper.compaction.queue.visibility.timeout.seconds")
            .description("The visibility timeout for the queue of compaction jobs.")
            .defaultValue("900")
            .propertyGroup(PropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS = named("sleeper.compaction.keepalive.period.seconds")
            .description("The frequency, in seconds, with which change message visibility requests are sent to extend the\n" +
                    "visibility of messages on the compaction job queue so that they are not processed by other processes.\n" +
                    "This should be less than the value of sleeper.queue.visibility.timeout.seconds.")
            .defaultValue("300")
            .propertyGroup(PropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES = named("sleeper.compaction.job.creation.period.minutes")
            .description("The rate at which the compaction job creation lambda runs (in minutes, must be >=1).")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(PropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_JOB_CREATION_LAMBDA_MEMORY_IN_MB = named("sleeper.compaction.job.creation.memory")
            .description("The amount of memory for the lambda that creates compaction jobs.")
            .defaultValue("1024")
            .propertyGroup(PropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS = named("sleeper.compaction.job.creation.timeout.seconds")
            .description("The timeout for the lambda that creates compaction jobs in seconds.")
            .defaultValue("900")
            .validationPredicate(Utils::isValidLambdaTimeout)
            .propertyGroup(PropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty MAXIMUM_CONCURRENT_COMPACTION_TASKS = named("sleeper.compaction.max.concurrent.tasks")
            .description("The maximum number of concurrent compaction tasks to run.")
            .defaultValue("300")
            .propertyGroup(PropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES = named("sleeper.compaction.task.creation.period.minutes")
            .description("The rate at which a check to see if compaction ECS tasks need to be created is made (in minutes, must be >= 1).")
            .defaultValue("1") // >0
            .propertyGroup(PropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_TASK_CPU_ARCHITECTURE = named("sleeper.compaction.task.cpu.architecture")
            .description("The CPU architecture to run compaction tasks on.")
            .defaultValue("X86_64")
            .propertyGroup(PropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_TASK_ARM_CPU = named("sleeper.compaction.task.arm.cpu")
            .description("The CPU for a compaction task using a 64 bit architecture.")
            .defaultValue("1024")
            .propertyGroup(PropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_TASK_ARM_MEMORY = named("sleeper.compaction.task.arm.memory")
            .description("The memory for a compaction task using a 64 bit architecture.")
            .defaultValue("4096")
            .propertyGroup(PropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_TASK_X86_CPU = named("sleeper.compaction.task.x86.cpu")
            .description("The CPU for a compaction task using a 32 bit architecture.")
            .defaultValue("1024")
            .propertyGroup(PropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_TASK_X86_MEMORY = named("sleeper.compaction.task.x86.memory")
            .description("The memory for a compaction task using a 32 bit architecture.")
            .defaultValue("4096")
            .propertyGroup(PropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_STATUS_STORE_ENABLED = named("sleeper.compaction.status.store.enabled")
            .description("Flag to enable/disable storage of tracking information for compaction jobs and tasks.")
            .defaultValue("true")
            .propertyGroup(PropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_JOB_STATUS_TTL_IN_SECONDS = named("sleeper.compaction.job.status.ttl")
            .description("The time to live in seconds for compaction job updates in the status store. Default is 1 week")
            .defaultValue("604800") // Default is 1 week
            .validationPredicate(Utils::isPositiveInteger)

            .propertyGroup(PropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_TASK_STATUS_TTL_IN_SECONDS = named("sleeper.compaction.task.status.ttl")
            .description("The time to live in seconds for compaction task updates in the status store. Default is 1 week")
            .defaultValue("604800") // Default is 1 week
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(PropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty DEFAULT_COMPACTION_STRATEGY_CLASS = named("sleeper.default.compaction.strategy.class")
            .description("\"The name of the class that defines how compaction jobs should be created.\n" +
                    "This should implement sleeper.compaction.strategy.CompactionStrategy. The value of this property is the\n" +
                    "default value which can be overridden on a per-table basis.")
            .defaultValue("sleeper.compaction.strategy.impl.SizeRatioCompactionStrategy")
            .propertyGroup(PropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty DEFAULT_COMPACTION_FILES_BATCH_SIZE = named("sleeper.default.compaction.files.batch.size")
            .description("The minimum number of files to read in a compaction job. Note that the state store\n" +
                    "must support atomic updates for this many files. For the DynamoDBStateStore this\n" +
                    "is 11. It can be overridden on a per-table basis.\n" +
                    "(NB This does not apply to splitting jobs which will run even if there is only 1 file.)\n" +
                    "This is a default value and will be used if not specified in the table.properties file")
            .defaultValue("11")
            .propertyGroup(PropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty DEFAULT_SIZERATIO_COMPACTION_STRATEGY_RATIO = named("sleeper.default.table.compaction.strategy.sizeratio.ratio")
            .description("Used by the SizeRatioCompactionStrategy to decide if a group of files should be compacted.\n" +
                    "If the file sizes are s_1, ..., s_n then the files are compacted if s_1 + ... + s_{n-1} >= ratio * s_n.\n" +
                    "It can be overridden on a per-table basis.")
            .defaultValue("3")
            .propertyGroup(PropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty DEFAULT_SIZERATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION = named("sleeper.default.table.compaction.strategy.sizeratio.max.concurrent.jobs.per.partition")
            .description("Used by the SizeRatioCompactionStrategy to control the maximum number of jobs that can be running\n" +
                    "concurrently per partition. It can be overridden on a per-table basis.")
            .defaultValue("" + Integer.MAX_VALUE)
            .propertyGroup(PropertyGroup.COMPACTION).build();

    // Query
    UserDefinedInstanceProperty MAXIMUM_CONNECTIONS_TO_S3_FOR_QUERIES = named("sleeper.query.s3.max-connections")
            .description("The maximum number of simultaneous connections to S3 from a single query runner. This is separated\n" +
                    "from the main one as it's common for a query runner to need to open more files at once.")
            .defaultValue("1024")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(PropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_PROCESSOR_LAMBDA_MEMORY_IN_MB = named("sleeper.query.processor.memory")
            .description("The amount of memory in MB for the lambda that executes queries.")
            .defaultValue("2048")
            .propertyGroup(PropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS = named("sleeper.query.processor.timeout.seconds")
            .description("The timeout for the lambda that executes queries in seconds.")
            .defaultValue("900")
            .propertyGroup(PropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_PROCESSING_LAMBDA_STATE_REFRESHING_PERIOD_IN_SECONDS = named("sleeper.query.processor.state.refresh.period.seconds")
            .description("The frequency with which the query processing lambda refreshes its knowledge of the system state\n" +
                    "(i.e. the partitions and the mapping from partition to files), in seconds.")
            .defaultValue("60")
            .propertyGroup(PropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_PROCESSING_LAMBDA_RESULTS_BATCH_SIZE = named("sleeper.query.processor.results.batch.size")
            .description("The maximum number of records to include in a batch of query results send to\n" +
                    "the results queue from the query processing lambda.")
            .defaultValue("2000")
            .propertyGroup(PropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_PROCESSOR_LAMBDA_RECORD_RETRIEVAL_THREADS = named("sleeper.query.processor.record.retrieval.threads")
            .description("The size of the thread pool for retrieving records in a query processing lambda.")
            .defaultValue("10")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(PropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_TRACKER_ITEM_TTL_IN_DAYS = named("sleeper.query.tracker.ttl.days")
            .description("This value is used to set the time-to-live on the tracking of the queries in the DynamoDB-based query tracker.")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(PropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_RESULTS_BUCKET_EXPIRY_IN_DAYS = named("sleeper.query.results.bucket.expiry.days")
            .description("The length of time the results of queries remain in the query results bucket before being deleted.")
            .defaultValue("7")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(PropertyGroup.QUERY).build();
    UserDefinedInstanceProperty DEFAULT_RESULTS_ROW_GROUP_SIZE = named("sleeper.default.query.results.rowgroup.size")
            .description("The default value of the rowgroup size used when the results of queries are written to Parquet files. The\n" +
                    "value given below is 8MiB. This value can be overridden using the query config.")
            .defaultValue("" + (8 * 1024 * 1024))
            // 8 MiB
            .propertyGroup(PropertyGroup.QUERY).build();
    UserDefinedInstanceProperty DEFAULT_RESULTS_PAGE_SIZE = named("sleeper.default.query.results.page.size")
            .description("The default value of the page size used when the results of queries are written to Parquet files. The\n" +
                    "value given below is 128KiB. This value can be overridden using the query config.")
            .defaultValue("" + (128 * 1024)) // 128 KiB
            .propertyGroup(PropertyGroup.QUERY).build();

    // Dashboard
    UserDefinedInstanceProperty DASHBOARD_TIME_WINDOW_MINUTES = named("sleeper.dashboard.time.window.minutes")
            .description("The period in minutes used in the dashboard.")
            .defaultValue("5")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(PropertyGroup.COMMON).build();

    // Logging levels
    UserDefinedInstanceProperty LOGGING_LEVEL = named("sleeper.logging.level")
            .description("The logging level for logging Sleeper classes. This does not apply to the MetricsLogger which is always set to INFO.")
            .propertyGroup(PropertyGroup.LOGGING).build();
    UserDefinedInstanceProperty APACHE_LOGGING_LEVEL = named("sleeper.logging.apache.level")
            .description("The logging level for Apache logs that are not Parquet.")
            .propertyGroup(PropertyGroup.LOGGING).build();
    UserDefinedInstanceProperty PARQUET_LOGGING_LEVEL = named("sleeper.logging.parquet.level")
            .description("The logging level for Parquet logs.")
            .propertyGroup(PropertyGroup.LOGGING).build();
    UserDefinedInstanceProperty AWS_LOGGING_LEVEL = named("sleeper.logging.aws.level")
            .description("The logging level for AWS logs.")
            .propertyGroup(PropertyGroup.LOGGING).build();
    UserDefinedInstanceProperty ROOT_LOGGING_LEVEL = named("sleeper.logging.root.level")
            .description("The logging level for everything else.")
            .propertyGroup(PropertyGroup.LOGGING).build();

    // Athena
    UserDefinedInstanceProperty SPILL_BUCKET_AGE_OFF_IN_DAYS = named("sleeper.athena.spill.bucket.ageoff.days")
            .description("The number of days before objects in the spill bucket are deleted.")
            .defaultValue("1")
            .propertyGroup(PropertyGroup.ATHENA).build();
    UserDefinedInstanceProperty ATHENA_COMPOSITE_HANDLER_CLASSES = named("sleeper.athena.handler.classes")
            .description("The fully qualified composite classes to deploy. These are the classes that interact with Athena.\n" +
                    "You can choose to remove one if you don't need them. Both are deployed by default.")
            .defaultValue("sleeper.athena.composite.SimpleCompositeHandler,sleeper.athena.composite.IteratorApplyingCompositeHandler")
            .propertyGroup(PropertyGroup.ATHENA).build();
    UserDefinedInstanceProperty ATHENA_COMPOSITE_HANDLER_MEMORY = named("sleeper.athena.handler.memory")
            .description("The amount of memory (GB) the athena composite handler has")
            .defaultValue("4096")
            .propertyGroup(PropertyGroup.ATHENA).build();
    UserDefinedInstanceProperty ATHENA_COMPOSITE_HANDLER_TIMEOUT_IN_SECONDS = named("sleeper.athena.handler.timeout.seconds")
            .description("The timeout in seconds for the athena composite handler")
            .defaultValue("900")
            .validationPredicate(Utils::isValidLambdaTimeout)
            .propertyGroup(PropertyGroup.ATHENA).build();

    // Default values
    UserDefinedInstanceProperty DEFAULT_S3A_READAHEAD_RANGE = named("sleeper.default.fs.s3a.readahead.range")
            .description("The readahead range set on the Hadoop configuration when reading Parquet files in a query\n" +
                    "(see https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html).")
            .defaultValue("64K")
            .propertyGroup(PropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_ROW_GROUP_SIZE = named("sleeper.default.rowgroup.size")
            .description("The size of the row group in the Parquet files (default is 8MiB).")
            .defaultValue("" + (8 * 1024 * 1024)) // 8 MiB
            .propertyGroup(PropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_PAGE_SIZE = named("sleeper.default.page.size")
            .description("The size of the pages in the Parquet files (default is 128KiB).")
            .defaultValue("" + (128 * 1024)) // 128 KiB
            .propertyGroup(PropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_COMPRESSION_CODEC = named("sleeper.default.compression.codec")
            .description("The compression codec to use in the Parquet files")
            .defaultValue("ZSTD")
            .validationPredicate(Utils::isValidCompressionCodec)
            .propertyGroup(PropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY_ENABLED = named("sleeper.default.table.dynamo.pointintimerecovery")
            .description("This specifies whether point in time recovery is turned on for DynamoDB tables. This default can\n" +
                    "be overridden by a table property.")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(PropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_DYNAMO_STRONGLY_CONSISTENT_READS = named("sleeper.default.table.dynamo.strongly.consistent.reads")
            .description("This specifies whether queries and scans against DynamoDB tables used in the DynamoDB state store\n" +
                    "are strongly consistent. This default can be overriden by a table property.")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(PropertyGroup.DEFAULT).build();

    static UserDefinedInstanceProperty[] values() {
        return UserDefinedInstancePropertyImpl.all().toArray(new UserDefinedInstanceProperty[0]);
    }

    static UserDefinedInstanceProperty from(String propertyName) {
        return UserDefinedInstancePropertyImpl.get(propertyName);
    }

    static boolean has(String propertyName) {
        return UserDefinedInstancePropertyImpl.get(propertyName) != null;
    }
}
