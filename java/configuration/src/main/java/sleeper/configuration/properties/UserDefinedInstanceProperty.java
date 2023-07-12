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
import sleeper.configuration.properties.table.CompressionCodec;
import sleeper.configuration.properties.validation.BatchIngestMode;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static sleeper.configuration.Utils.describeEnumValuesInLowerCase;

/**
 * Sleeper properties set by the user. All non-mandatory properties should be accompanied by a default value and should
 * have a validation predicate for determining if the value a user has provided is valid. By default the predicate
 * always returns true indicating the property is valid.
 */
// Suppress as this class will always be referenced before impl class, so initialization behaviour will be deterministic
@SuppressFBWarnings("IC_SUPERCLASS_USES_SUBCLASS_DURING_INITIALIZATION")
public interface UserDefinedInstanceProperty extends InstanceProperty {
    // Common
    UserDefinedInstanceProperty ID = Index.propertyBuilder("sleeper.id")
            .description("A string to uniquely identify this deployment. This should be no longer than 20 chars. " +
                    "It should be globally unique as it will be used to name AWS resources such as S3 buckets.")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .editable(false).build();
    UserDefinedInstanceProperty JARS_BUCKET = Index.propertyBuilder("sleeper.jars.bucket")
            .description("The S3 bucket containing the jar files of the Sleeper components.")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty USER_JARS = Index.propertyBuilder("sleeper.userjars")
            .description("A comma-separated list of the jars containing application specific iterator code. " +
                    "These jars are assumed to be in the bucket given by sleeper.jars.bucket, e.g. if that " +
                    "bucket contains two iterator jars called iterator1.jar and iterator2.jar then the " +
                    "property should be 'sleeper.userjars=iterator1.jar,iterator2.jar'.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .includedInBasicTemplate(true).build();
    UserDefinedInstanceProperty TAGS = Index.propertyBuilder("sleeper.tags")
            .description("A list of tags for the project.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCDKDeployWhenChanged(true)
            .includedInTemplate(false).build();
    UserDefinedInstanceProperty STACK_TAG_NAME = Index.propertyBuilder("sleeper.stack.tag.name")
            .description("A name for a tag to identify the stack that deployed a resource. This will be set for all AWS resources, to the ID of " +
                    "the CDK stack that they are deployed under. This can be used to organise the cost explorer for billing.")
            .defaultValue("DeploymentStack")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty RETAIN_INFRA_AFTER_DESTROY = Index.propertyBuilder("sleeper.retain.infra.after.destroy")
            .description("Whether to keep the sleeper table bucket, Dynamo tables, query results bucket, etc., " +
                    "when the instance is destroyed.")
            .defaultValue("true")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCDKDeployWhenChanged(true)
            .includedInBasicTemplate(true).build();
    UserDefinedInstanceProperty OPTIONAL_STACKS = Index.propertyBuilder("sleeper.optional.stacks")
            .description("The optional stacks to deploy.")
            .defaultValue("CompactionStack,GarbageCollectorStack,IngestStack,PartitionSplittingStack,QueryStack,AthenaStack,EmrServerlessBulkImportStack,DashboardStack")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCDKDeployWhenChanged(true)
            .includedInBasicTemplate(true).build();
    UserDefinedInstanceProperty ACCOUNT = Index.propertyBuilder("sleeper.account")
            .description("The AWS account number. This is the AWS account that the instance will be deployed to.")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .editable(false).build();
    UserDefinedInstanceProperty REGION = Index.propertyBuilder("sleeper.region")
            .description("The AWS region to deploy to.")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .editable(false).build();
    UserDefinedInstanceProperty VPC_ID = Index.propertyBuilder("sleeper.vpc")
            .description("The id of the VPC to deploy to.")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .editable(false).build();
    UserDefinedInstanceProperty VPC_ENDPOINT_CHECK = Index.propertyBuilder("sleeper.vpc.endpoint.check")
            .description("Whether to check that the VPC that the instance is deployed to has an S3 endpoint. " +
                    "If there is no S3 endpoint then the NAT costs can be very significant.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    UserDefinedInstanceProperty SUBNETS = Index.propertyBuilder("sleeper.subnets")
            .description("A comma separated list of subnets to deploy to. ECS tasks will be run across multiple " +
                    "subnets. EMR clusters will be deployed in a subnet chosen when the cluster is created.")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .editable(false).build();
    UserDefinedInstanceProperty FILE_SYSTEM = Index.propertyBuilder("sleeper.filesystem")
            .description("The Hadoop filesystem used to connect to S3.")
            .defaultValue("s3a://")
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    UserDefinedInstanceProperty EMAIL_ADDRESS_FOR_ERROR_NOTIFICATION = Index.propertyBuilder("sleeper.errors.email")
            .description("An email address used by the TopicStack to publish SNS notifications of errors.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCDKDeployWhenChanged(true)
            .includedInBasicTemplate(true).build();
    UserDefinedInstanceProperty QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.queue.visibility.timeout.seconds")
            .description("The visibility timeout on the queues used in ingest, query, etc.")
            .defaultValue("900")
            .validationPredicate(Utils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty LOG_RETENTION_IN_DAYS = Index.propertyBuilder("sleeper.log.retention.days")
            .description("The length of time in days that CloudWatch logs from lambda functions, ECS containers, etc., are retained.\n" +
                    "See https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-logs-loggroup.html for valid options.\n" +
                    "Use -1 to indicate infinite retention.")
            .defaultValue("30")
            .validationPredicate(Utils::isValidLogRetention)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty MAXIMUM_CONNECTIONS_TO_S3 = Index.propertyBuilder("sleeper.s3.max-connections")
            .description("Used to set the value of fs.s3a.connection.maximum on the Hadoop configuration. This controls the " +
                    "maximum number of http connections to S3.\n" +
                    "See https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/performance.html")
            .defaultValue("25")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    UserDefinedInstanceProperty FARGATE_VERSION = Index.propertyBuilder("sleeper.fargate.version")
            .description("The version of Fargate to use.")
            .defaultValue("1.4.0")
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    UserDefinedInstanceProperty TASK_RUNNER_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.task.runner.memory")
            .description("The amount of memory for the lambda that creates ECS tasks to execute compaction and ingest jobs.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.task.runner.timeout.seconds")
            .description("The timeout in seconds for the lambda that creates ECS tasks to execute compaction jobs and ingest jobs.\n" +
                    "This must be >0 and <= 900.")
            .defaultValue("900")
            .validationPredicate(Utils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty METRICS_NAMESPACE = Index.propertyBuilder("sleeper.metrics.namespace")
            .description("The namespaces for the metrics used in the metrics stack.")
            .defaultValue("Sleeper")
            .validationPredicate(Utils::isNonNullNonEmptyString)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCDKDeployWhenChanged(true).build();

    // Ingest
    UserDefinedInstanceProperty ECR_INGEST_REPO = Index.propertyBuilder("sleeper.ingest.repo")
            .description("The name of the ECR repository for the ingest container. The Docker image from the ingest module should have been " +
                    "uploaded to an ECR repository of this name in this account.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty MAXIMUM_CONCURRENT_INGEST_TASKS = Index.propertyBuilder("sleeper.ingest.max.concurrent.tasks")
            .description("The maximum number of concurrent ECS tasks to run.")
            .defaultValue("200")
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_TASK_CREATION_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.ingest.task.creation.period.minutes")
            .description("The frequency in minutes with which an EventBridge rule runs to trigger a lambda that, if necessary, runs more ECS " +
                    "tasks to perform ingest jobs.")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_KEEP_ALIVE_PERIOD_IN_SECONDS = Index.propertyBuilder("sleeper.ingest.keepalive.period.seconds")
            .description("The frequency, in seconds, with which change message visibility requests are sent to extend the " +
                    "visibility of messages on the ingest queue so that they are not processed by other processes.\n" +
                    "This should be less than the value of sleeper.queue.visibility.timeout.seconds.")
            .defaultValue("300")
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty S3A_INPUT_FADVISE = Index.propertyBuilder("sleeper.ingest.fs.s3a.experimental.input.fadvise")
            .description("This sets the value of fs.s3a.experimental.input.fadvise on the Hadoop configuration used to read and write " +
                    "files to and from S3 in ingest jobs. Changing this value allows you to fine-tune how files are read. Possible " +
                    "values are \"normal\", \"sequential\" and \"random\". More information is available here:\n" +
                    "https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/performance.html#fadvise.")
            .defaultValue("sequential")
            .validationPredicate(Utils::isValidFadvise)
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_TASK_CPU = Index.propertyBuilder("sleeper.ingest.task.cpu")
            .description("The amount of CPU used by Fargate tasks that perform ingest jobs.\n" +
                    "Note that only certain combinations of CPU and memory are valid.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("2048")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_TASK_MEMORY = Index.propertyBuilder("sleeper.ingest.task.memory")
            .description("The amount of memory used by Fargate tasks that perform ingest jobs.\n" +
                    "Note that only certain combinations of CPU and memory are valid.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("4096")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS = Index.propertyBuilder("sleeper.ingest.partition.refresh.period")
            .description("The frequency in seconds with which ingest tasks refresh their view of the partitions.\n" +
                    "(NB Refreshes only happen once a batch of data has been written so this is a lower bound " +
                    "on the refresh frequency.)")
            .defaultValue("120")
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_SOURCE_BUCKET = Index.propertyBuilder("sleeper.ingest.source.bucket")
            .description("A comma-separated list of buckets that contain files to be ingested via ingest jobs. The buckets should already " +
                    "exist, i.e. they will not be created as part of the cdk deployment of this instance of Sleeper. The ingest " +
                    "and bulk import stacks will be given read access to these buckets so that they can consume data from them.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_RECORD_BATCH_TYPE = Index.propertyBuilder("sleeper.ingest.record.batch.type")
            .description("The way in which records are held in memory before they are written to a local store.\n" +
                    "Valid values are 'arraylist' and 'arrow'.\n" +
                    "The arraylist method is simpler, but it is slower and requires careful tuning of the number of records in each batch.")
            .defaultValue("arrow")
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_PARTITION_FILE_WRITER_TYPE = Index.propertyBuilder("sleeper.ingest.partition.file.writer.type")
            .description("The way in which partition files are written to the main Sleeper store.\n" +
                    "Valid values are 'direct' (which writes using the s3a Hadoop file system) and 'async' (which writes locally and then " +
                    "copies the completed Parquet file asynchronously into S3).\n" +
                    "The direct method is simpler but the async method should provide better performance when the number of partitions " +
                    "is large.")
            .defaultValue("async")
            .propertyGroup(InstancePropertyGroup.INGEST).build();

    // ArrayList ingest
    UserDefinedInstanceProperty MAX_RECORDS_TO_WRITE_LOCALLY = Index.propertyBuilder("sleeper.ingest.max.local.records")
            .description("The maximum number of records written to local file in an ingest job. (Records are written in sorted order to local " +
                    "disk before being uploaded to S3. Increasing this value increases the amount of time before data is visible in the " +
                    "system, but increases the number of records written to S3 in a batch, therefore reducing costs.)\n" +
                    "(arraylist-based ingest only)")
            .defaultValue("100000000")
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty MAX_IN_MEMORY_BATCH_SIZE = Index.propertyBuilder("sleeper.ingest.memory.max.batch.size")
            .description("The maximum number of records to read into memory in an ingest job. (Up to sleeper.ingest.memory.max.batch.size " +
                    "records are read into memory before being sorted and written to disk. This process is repeated until " +
                    "sleeper.ingest.max.local.records records have been written to local files. Then the sorted files and merged and " +
                    "the data is written to sorted files in S3.)\n" +
                    "(arraylist-based ingest only)")
            .defaultValue("1000000")
            .propertyGroup(InstancePropertyGroup.INGEST).build();

    // Arrow ingest
    UserDefinedInstanceProperty ARROW_INGEST_WORKING_BUFFER_BYTES = Index.propertyBuilder("sleeper.ingest.arrow.working.buffer.bytes")
            .description("The number of bytes to allocate to the Arrow working buffer. This buffer is used for sorting and other sundry " +
                    "activities. " +
                    "Note that this is off-heap memory, which is in addition to the memory assigned to the JVM.\n" +
                    "(arrow-based ingest only) [256MB]")
            .defaultValue("268435456")
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty ARROW_INGEST_BATCH_BUFFER_BYTES = Index.propertyBuilder("sleeper.ingest.arrow.batch.buffer.bytes")
            .description("The number of bytes to allocate to the Arrow batch buffer, which is used to hold the records before they are " +
                    "written to local disk. A larger value means that the local disk holds fewer, larger files, which are more efficient " +
                    "to merge together during an upload to S3. Larger values may require a larger working buffer. " +
                    "Note that this is off-heap memory, which is in addition to the memory assigned to the JVM.\n" +
                    "(arrow-based ingest only) [1GB]")
            .defaultValue("1073741824")
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty ARROW_INGEST_MAX_LOCAL_STORE_BYTES = Index.propertyBuilder("sleeper.ingest.arrow.max.local.store.bytes")
            .description("The maximum number of bytes to store on the local disk before uploading to the main Sleeper store. A larger value " +
                    "reduces the number of S3 PUTs that are required to upload thle data to S3 and results in fewer files per partition.\n" +
                    "(arrow-based ingest only) [2GB]")
            .defaultValue("2147483648")
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS = Index.propertyBuilder("sleeper.ingest.arrow.max.single.write.to.file.records")
            .description("The number of records to write at once into an Arrow file in the local store. A single Arrow file contains many of " +
                    "these micro-batches and so this parameter does not significantly affect the final size of the Arrow file. " +
                    "Larger values may require a larger working buffer.\n" +
                    "(arrow-based ingest only) [1K]")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.INGEST).build();

    // Async ingest partition file writer
    UserDefinedInstanceProperty ASYNC_INGEST_CLIENT_TYPE = Index.propertyBuilder("sleeper.ingest.async.client.type")
            .description("The implementation of the async S3 client to use for upload during ingest.\n" +
                    "Valid values are 'java' or 'crt'. This determines the implementation of S3AsyncClient that gets used.\n" +
                    "With 'java' it makes a single PutObject request for each file.\n" +
                    "With 'crt' it uses the AWS Common Runtime (CRT) to make multipart uploads.\n" +
                    "Note that the CRT option is recommended. Using the Java option may cause failures if any file is >5GB in size, and " +
                    "will lead to the following warning:\n" +
                    "\"The provided S3AsyncClient is not an instance of S3CrtAsyncClient, and thus multipart upload/download feature is not " +
                    "enabled and resumable file upload is not supported. To benefit from maximum throughput, consider using " +
                    "S3AsyncClient.crtBuilder().build() instead.\"\n" +
                    "(async partition file writer only)")
            .defaultValue("crt")
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty ASYNC_INGEST_CRT_PART_SIZE_BYTES = Index.propertyBuilder("sleeper.ingest.async.crt.part.size.bytes")
            .description("The part size in bytes to use for multipart uploads.\n" +
                    "(CRT async ingest only) [128MB]")
            .defaultValue("134217728") // 128M
            .validationPredicate(Utils::isPositiveLong)
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty ASYNC_INGEST_CRT_TARGET_THROUGHPUT_GBPS = Index.propertyBuilder("sleeper.ingest.async.crt.target.throughput.gbps")
            .description("The target throughput for multipart uploads, in GB/s. Determines how many parts should be uploaded simultaneously.\n" +
                    "(CRT async ingest only)")
            .defaultValue("10")
            .validationPredicate(Utils::isPositiveDouble)
            .propertyGroup(InstancePropertyGroup.INGEST).build();

    // Status Store
    UserDefinedInstanceProperty INGEST_STATUS_STORE_ENABLED = Index.propertyBuilder("sleeper.ingest.status.store.enabled")
            .description("Flag to enable/disable storage of tracking information for ingest jobs and tasks.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_JOB_STATUS_TTL_IN_SECONDS = Index.propertyBuilder("sleeper.ingest.job.status.ttl")
            .description("The time to live in seconds for ingest job updates in the status store. Default is 1 week.\n" +
                    "The expiry time is fixed when an update is saved to the store, so changing this will only affect new data.")
            .defaultValue("604800") // Default is 1 week
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_TASK_STATUS_TTL_IN_SECONDS = Index.propertyBuilder("sleeper.ingest.task.status.ttl")
            .description("The time to live in seconds for ingest task updates in the status store. Default is 1 week.\n" +
                    "The expiry time is fixed when an update is saved to the store, so changing this will only affect new data.")
            .defaultValue("604800") // Default is 1 week
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.INGEST).build();

    // Batcher
    UserDefinedInstanceProperty INGEST_BATCHER_SUBMITTER_MEMORY_IN_MB = Index.propertyBuilder("sleeper.ingest.batcher.submitter.memory.mb")
            .description("The amount of memory in MB for the lambda that receives submitted requests to ingest files.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_BATCHER_SUBMITTER_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.ingest.batcher.submitter.timeout.seconds")
            .description("The timeout in seconds for the lambda that receives submitted requests to ingest files.")
            .defaultValue("20")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_BATCHER_JOB_CREATION_MEMORY_IN_MB = Index.propertyBuilder("sleeper.ingest.batcher.job.creation.memory.mb")
            .description("The amount of memory in MB for the lambda that creates ingest jobs from submitted file ingest requests.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_BATCHER_JOB_CREATION_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.ingest.batcher.job.creation.timeout.seconds")
            .description("The timeout in seconds for the lambda that creates ingest jobs from submitted file ingest requests.")
            .defaultValue("900")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_BATCHER_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.ingest.batcher.job.creation.period.minutes")
            .description("The rate at which the ingest batcher job creation lambda runs (in minutes, must be >=1).")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCDKDeployWhenChanged(true).build();

    // Bulk Import - properties that are applicable to all bulk import platforms
    UserDefinedInstanceProperty BULK_IMPORT_CLASS_NAME = Index.propertyBuilder("sleeper.bulk.import.class.name")
            .description("The class to use to perform the bulk import. The default value below uses Spark Dataframes. There is an " +
                    "alternative option that uses RDDs (sleeper.bulkimport.job.runner.rdd.BulkImportJobRDDDriver).")
            .defaultValue("sleeper.bulkimport.job.runner.dataframelocalsort.BulkImportDataframeLocalSortDriver")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_SHUFFLE_MAPSTATUS_COMPRESSION_CODEC = Index.propertyBuilder("sleeper.bulk.import.emr.spark.shuffle.mapStatus.compression.codec")
            .description("The compression codec for map status results. Used to set spark.shuffle.mapStatus.compression.codec.\n" +
                    "Stops \"Decompression error: Version not supported\" errors - only a value of \"lz4\" has been tested.")
            .defaultValue("lz4")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_SPECULATION = Index.propertyBuilder("sleeper.bulk.import.emr.spark.speculation")
            .description("If true then speculative execution of tasks will be performed. Used to set spark.speculation.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_SPECULATION_QUANTILE = Index.propertyBuilder("sleeper.bulk.import.spark.speculation.quantile")
            .description("Fraction of tasks which must be complete before speculation is enabled for a particular stage. Used to set spark.speculation.quantile.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("0.75")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();

    // Bulk import using EMR - these properties are used by both the persistent
    // and non-persistent EMR stacks

    //  - The following properties depend on the instance type and number of instances - they have been chosen
    //          based on the default settings for the EMR and persistent EMR clusters (these are currently the
    //          same which allows the following properties to be used across both types):
    //      - Theses are based on this blog
    //      https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/
    //      - Our default core/task instance type is m6i.4xlarge. These have 64GB of RAM and 16 vCPU. The amount of
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
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_EXECUTOR_MEMORY = Index.propertyBuilder("sleeper.bulk.import.emr.spark.executor.memory")
            .description("The amount of memory allocated to a Spark executor. Used to set spark.executor.memory.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("16g")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_DRIVER_MEMORY = Index.propertyBuilder("sleeper.bulk.import.emr.spark.driver.memory")
            .description("The amount of memory allocated to the Spark driver. Used to set spark.driver.memory.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EMR_SPARK_EXECUTOR_MEMORY.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_EXECUTOR_INSTANCES = Index.propertyBuilder("sleeper.bulk.import.emr.spark.executor.instances")
            .description("The number of executors. Used to set spark.executor.instances.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("29")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD = Index.propertyBuilder("sleeper.bulk.import.emr.spark.yarn.executor.memory.overhead")
            .description("The memory overhead for an executor. Used to set spark.yarn.executor.memoryOverhead.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("2g")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_YARN_DRIVER_MEMORY_OVERHEAD = Index.propertyBuilder("sleeper.bulk.import.emr.spark.yarn.driver.memory.overhead")
            .description("The memory overhead for the driver. Used to set spark.yarn.driver.memoryOverhead.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EMR_SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_DEFAULT_PARALLELISM = Index.propertyBuilder("sleeper.bulk.import.emr.spark.default.parallelism")
            .description("The default parallelism for Spark job. Used to set spark.default.parallelism.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("290")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_SQL_SHUFFLE_PARTITIONS = Index.propertyBuilder("sleeper.bulk.import.emr.spark.sql.shuffle.partitions")
            .description("The number of partitions used in a Spark SQL/dataframe shuffle operation. Used to set spark.sql.shuffle.partitions.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EMR_SPARK_DEFAULT_PARALLELISM.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    //  - Properties that are independent of the instance type and number of instances:
    UserDefinedInstanceProperty BULK_IMPORT_EMR_EC2_KEYPAIR_NAME = Index.propertyBuilder("sleeper.bulk.import.emr.keypair.name")
            .description("(Non-persistent or persistent EMR mode only) An EC2 keypair to use for the EC2 instances. Specifying this will allow you to SSH to the nodes " +
                    "in the cluster while it's running.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP = Index.propertyBuilder("sleeper.bulk.import.emr.master.additional.security.group")
            .description("(Non-persistent or persistent EMR mode only) Specifying this security group causes the group " +
                    "to be added to the EMR master's list of security groups.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_EXECUTOR_CORES = Index.propertyBuilder("sleeper.bulk.import.emr.spark.executor.cores")
            .description("(Non-persistent or persistent EMR mode only) The number of cores used by an executor. Used to set spark.executor.cores.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("5")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_DRIVER_CORES = Index.propertyBuilder("sleeper.bulk.import.emr.spark.driver.cores")
            .description("(Non-persistent or persistent EMR mode only) The number of cores used by the driver. Used to set spark.driver.cores.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EMR_SPARK_EXECUTOR_CORES.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_NETWORK_TIMEOUT = Index.propertyBuilder("sleeper.bulk.import.emr.spark.network.timeout")
            .description("(Non-persistent or persistent EMR mode only) The default timeout for network interactions in Spark. " +
                    "Used to set spark.network.timeout.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("800s")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_EXECUTOR_HEARTBEAT_INTERVAL = Index.propertyBuilder("sleeper.bulk.import.emr.spark.executor.heartbeat.interval")
            .description("(Non-persistent or persistent EMR mode only) The interval between heartbeats from executors to the driver. " +
                    "Used to set spark.executor.heartbeatInterval.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("60s")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_DYNAMIC_ALLOCATION_ENABLED = Index.propertyBuilder("sleeper.bulk.import.emr.spark.dynamic.allocation.enabled")
            .description("(Non-persistent or persistent EMR mode only) Whether Spark should use dynamic allocation to scale resources up and down. " +
                    "Used to set spark.dynamicAllocation.enabled.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("false")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_MEMORY_FRACTION = Index.propertyBuilder("sleeper.bulk.import.emr.spark.memory.fraction")
            .description("(Non-persistent or persistent EMR mode only) The fraction of heap space used for execution and storage. " +
                    "Used to set spark.memory.fraction.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("0.80")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_MEMORY_STORAGE_FRACTION = Index.propertyBuilder("sleeper.bulk.import.emr.spark.memory.storage.fraction")
            .description("(Non-persistent or persistent EMR mode only) The amount of storage memory immune to eviction, " +
                    "expressed as a fraction of the heap space used for execution and storage. " +
                    "Used to set spark.memory.storageFraction.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("0.30")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS = Index.propertyBuilder("sleeper.bulk.import.emr.spark.executor.extra.java.options")
            .description("(Non-persistent or persistent EMR mode only) JVM options passed to the executors. " +
                    "Used to set spark.executor.extraJavaOptions.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_DRIVER_EXTRA_JAVA_OPTIONS = Index.propertyBuilder("sleeper.bulk.import.emr.spark.driver.extra.java.options")
            .description("(Non-persistent or persistent EMR mode only) JVM options passed to the driver. " +
                    "Used to set spark.driver.extraJavaOptions.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EMR_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_YARN_SCHEDULER_REPORTER_THREAD_MAX_FAILURES = Index.propertyBuilder("sleeper.bulk.import.emr.spark.yarn.scheduler.reporter.thread.max.failures")
            .description("(Non-persistent or persistent EMR mode only) The maximum number of executor failures before YARN can fail the application. " +
                    "Used to set spark.yarn.scheduler.reporterThread.maxFailures.\n" +
                    "See https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/.")
            .defaultValue("5")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_STORAGE_LEVEL = Index.propertyBuilder("sleeper.bulk.import.emr.spark.storage.level")
            .description("(Non-persistent or persistent EMR mode only) The storage to use for temporary caching. " +
                    "Used to set spark.storage.level.\n" +
                    "See https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/.")
            .defaultValue("MEMORY_AND_DISK_SER")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_RDD_COMPRESS = Index.propertyBuilder("sleeper.bulk.import.emr.spark.rdd.compress")
            .description("(Non-persistent or persistent EMR mode only) Whether to compress serialized RDD partitions. " +
                    "Used to set spark.rdd.compress.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_SHUFFLE_COMPRESS = Index.propertyBuilder("sleeper.bulk.import.emr.spark.shuffle.compress")
            .description("(Non-persistent or persistent EMR mode only) Whether to compress map output files. " +
                    "Used to set spark.shuffle.compress.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_SHUFFLE_SPILL_COMPRESS = Index.propertyBuilder("sleeper.bulk.import.emr.spark.shuffle.spill.compress")
            .description("(Non-persistent or persistent EMR mode only) Whether to compress data spilled during shuffles. " +
                    "Used to set spark.shuffle.spill.compress.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_EBS_VOLUME_SIZE_IN_GB = Index.propertyBuilder("sleeper.bulk.import.emr.ebs.volume.size.gb")
            .description("(Non-persistent or persistent EMR mode only) The size of the EBS volume in gibibytes (GiB).\n" +
                    "This can be a number from 10 to 1024.")
            .defaultValue("256")
            .validationPredicate(Utils::isValidEbsSize)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_EBS_VOLUME_TYPE = Index.propertyBuilder("sleeper.bulk.import.emr.ebs.volume.type")
            .description("(Non-persistent or persistent EMR mode only) The type of the EBS volume.\n" +
                    "Valid values are 'gp2', 'gp3', 'io1', 'io2'.")
            .defaultValue("gp2")
            .validationPredicate(Utils::isValidEbsVolumeType)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_EBS_VOLUMES_PER_INSTANCE = Index.propertyBuilder("sleeper.bulk.import.emr.ebs.volumes.per.instance")
            .description("(Non-persistent or persistent EMR mode only) The number of EBS volumes per instance.\n" +
                    "This can be a number from 1 to 25.")
            .defaultValue("4").validationPredicate(s -> Utils.isPositiveIntLtEqValue(s, 25))
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();

    // Bulk import using the non-persistent EMR approach
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL = Index.propertyBuilder("sleeper.default.bulk.import.emr.release.label")
            .description("(Non-persistent EMR mode only) The default EMR release label to be used when creating an EMR cluster for bulk importing data " +
                    "using Spark running on EMR.\n" +
                    "This property is a default which can be overridden by a table property or by a property in the " +
                    "bulk import job specification.")
            .defaultValue("emr-6.10.0")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES = Index.propertyBuilder("sleeper.default.bulk.import.emr.master.x86.instance.types")
            .description("(Non-persistent EMR mode only) The default EC2 x86 instance types to be used for the master " +
                    "node of the EMR cluster. " +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .defaultValue("m6i.xlarge")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES = Index.propertyBuilder("sleeper.default.bulk.import.emr.executor.x86.instance.types")
            .description("(Non-persistent EMR mode only) The default EC2 x86_64 instance types to be used for the executor " +
                    "nodes of the EMR cluster. " +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .defaultValue("m6i.4xlarge")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_MASTER_ARM_INSTANCE_TYPES = Index.propertyBuilder("sleeper.default.bulk.import.emr.master.arm.instance.types")
            .description("(Non-persistent EMR mode only) The default EC2 ARM64 instance types to be used for the master " +
                    "node of the EMR cluster. " +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_EXECUTOR_ARM_INSTANCE_TYPES = Index.propertyBuilder("sleeper.default.bulk.import.emr.executor.arm.instance.types")
            .description("(Non-persistent EMR mode only) The default EC2 ARM64 instance types to be used for the executor " +
                    "nodes of the EMR cluster. " +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE = Index.propertyBuilder("sleeper.default.bulk.import.emr.executor.market.type")
            .description("(Non-persistent EMR mode only) The default purchasing option to be used for the executor " +
                    "nodes of the EMR cluster.\n" +
                    "Valid values are ON_DEMAND or SPOT.\n" +
                    "This property is a default which can be overridden by a table property or by a property in the " +
                    "bulk import job specification.")
            .defaultValue("SPOT").validationPredicate(s -> ("SPOT".equals(s) || "ON_DEMAND".equals(s)))
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY = Index.propertyBuilder("sleeper.default.bulk.import.emr.executor.initial.instances")
            .description("(Non-persistent EMR mode only) The default initial number of capacity units to provision as EC2 " +
                    "instances for executors in the EMR cluster.\n" +
                    "This is measured in instance fleet capacity units. These are declared alongside the requested " +
                    "instance types, as each type will count for a certain number of units. By default the units are " +
                    "the number of instances.\n" +
                    "This property is a default which can be overridden by a table property or by a property in the " +
                    "bulk import job specification.")
            .defaultValue("2")
            .validationPredicate(Utils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY = Index.propertyBuilder("sleeper.default.bulk.import.emr.executor.max.instances")
            .description("(Non-persistent EMR mode only) The default maximum number of capacity units to provision as EC2 " +
                    "instances for executors in the EMR cluster.\n" +
                    "This is measured in instance fleet capacity units. These are declared alongside the requested " +
                    "instance types, as each type will count for a certain number of units. By default the units are " +
                    "the number of instances.\n" +
                    "This property is a default which can be overridden by a table property or by a property in the " +
                    "bulk import job specification.")
            .defaultValue("10")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();

    // Bulk import using a persistent EMR cluster
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_RELEASE_LABEL = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.release.label")
            .description("(Persistent EMR mode only) The EMR release used to create the persistent EMR cluster.")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MASTER_X86_INSTANCE_TYPES = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.master.x86.instance.types")
            .description("(Persistent EMR mode only) The EC2 x86 instance types used for the master node of the " +
                    "persistent EMR cluster. " +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_X86_INSTANCE_TYPES = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.executor.x86.instance.types")
            .description("(Persistent EMR mode only) The EC2 x86 instance types used for the executor nodes of the " +
                    "persistent EMR cluster. " +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MASTER_ARM_INSTANCE_TYPES = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.master.arm.instance.types")
            .description("(Persistent EMR mode only) The EC2 ARM64 instance types used for the master node of the " +
                    "persistent EMR cluster. " +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_MASTER_ARM_INSTANCE_TYPES.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_ARM_INSTANCE_TYPES = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.executor.arm.instance.types")
            .description("(Persistent EMR mode only) The EC2 ARM64 instance types used for the executor nodes of the " +
                    "persistent EMR cluster. " +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_EXECUTOR_ARM_INSTANCE_TYPES.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.use.managed.scaling")
            .description("(Persistent EMR mode only) Whether the persistent EMR cluster should use managed scaling or not.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.min.capacity")
            .description("(Persistent EMR mode only) The minimum number of capacity units to provision as EC2 " +
                    "instances for executors in the persistent EMR cluster.\n" +
                    "This is measured in instance fleet capacity units. These are declared alongside the requested " +
                    "instance types, as each type will count for a certain number of units. By default the units are " +
                    "the number of instances.\n" +
                    "If managed scaling is not used then the cluster will be of fixed size, with a number of " +
                    "instances equal to this value.")
            .defaultValue("1")
            .validationPredicate(Utils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.max.capacity")
            .description("(Persistent EMR mode only) The maximum number of capacity units to provision as EC2 " +
                    "instances for executors in the persistent EMR cluster.\n" +
                    "This is measured in instance fleet capacity units. These are declared alongside the requested " +
                    "instance types, as each type will count for a certain number of units. By default the units are " +
                    "the number of instances.\n" +
                    "This value is only used if managed scaling is used.")
            .defaultValue("10")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_STEP_CONCURRENCY_LEVEL = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.step.concurrency.level")
            .description("(Persistent EMR mode only) This controls the number of EMR steps that can run concurrently.")
            .defaultValue("2")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();

    // Bulk import using EMR Serverless
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_CLASS_NAME = Index.propertyBuilder("sleeper.bulk.import.emrserverless.class.name")
            .description("The class to use to perform the bulk import.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .defaultValue(null)
            .runCDKDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.architecture")
            .description("The architecture for EMR Serverless to use. X86_64 or ARM (Coming soon)")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .defaultValue("X86_64")
            .runCDKDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_ENABLED = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.enabled")
            .description("The switch to enable EMR Serverless over persistent EMR.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .defaultValue("true")
            .runCDKDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_RELEASE = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.release")
            .description("The version of EMR Serverless to use.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .defaultValue("emr-6.10.0")
            .runCDKDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.repo")
            .description("The name of the repository for the EMR serverless container. The Docker image from the bulk-import module " +
                    "should have been uploaded to an ECR repository of this name in this account.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_TYPE = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.type")
            .description("The type of EMR Serverless to use. Spark or Hive")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .defaultValue("Spark")
            .runCDKDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_CORES = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.executor.cores")
            .description("The number of cores used by an Serverless executor. Used to set spark.executor.cores.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("5")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_MEMORY = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.executor.memory")
            .description("The amount of memory allocated to a Serverless executor. Used to set spark.executor.memory.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("16g")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_INSTANCES = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.executor.instances")
            .description("The number of executors to be used with Serverless. Used to set spark.executor.instances.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("29")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_DRIVER_CORES = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.driver.cores")
            .description("The number of cores used by the Serverless Spark driver. Used to set spark.driver.cores.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_CORES.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_DRIVER_MEMORY = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.driver.memory")
            .description("The amount of memory allocated to the Serverless Spark driver. Used to set spark.driver.memory.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_MEMORY.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();

    // Bulk import using EKS
    UserDefinedInstanceProperty BULK_IMPORT_REPO = Index.propertyBuilder("sleeper.bulk.import.eks.repo")
            .description("(EKS mode only) The name of the ECS repository where the Docker image for the bulk import container is stored.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();

    // Partition splitting
    UserDefinedInstanceProperty PARTITION_SPLITTING_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.partition.splitting.period.minutes")
            .description("The frequency in minutes with which the lambda that finds partitions that need splitting runs.")
            .defaultValue("30")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB = Index.propertyBuilder("sleeper.partition.splitting.files.maximum")
            .description("When a partition needs splitting, a partition splitting job is created. This reads in the sketch files " +
                    "associated to the files in the partition in order to identify the median. This parameter controls the " +
                    "maximum number of files that are read in.")
            .defaultValue("50")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING).build();
    UserDefinedInstanceProperty FIND_PARTITIONS_TO_SPLIT_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.partition.splitting.finder.memory")
            .description("The amount of memory in MB for the lambda function used to identify partitions that need to be split.")
            .defaultValue("2048")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty FIND_PARTITIONS_TO_SPLIT_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.partition.splitting.finder.timeout.seconds")
            .description("The timeout in seconds for the lambda function used to identify partitions that need to be split.")
            .defaultValue("900")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty SPLIT_PARTITIONS_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.partition.splitting.memory")
            .description("The memory for the lambda function used to split partitions.")
            .defaultValue("2048")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty SPLIT_PARTITIONS_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.partition.splitting.timeout.seconds")
            .description("The timeout in seconds for the lambda function used to split partitions.")
            .defaultValue("900")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty DEFAULT_PARTITION_SPLIT_THRESHOLD = Index.propertyBuilder("sleeper.default.partition.splitting.threshold")
            .description("This is the default value of the partition splitting threshold. Partitions with more than the following " +
                    "number of records in will be split. This value can be overridden on a per-table basis.")
            .defaultValue("1000000000")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING).build();

    // Garbage collection
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.gc.period.minutes")
            .description("The frequency in minutes with which the garbage collector lambda is run.")
            .defaultValue("15")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.gc.memory")
            .description("The memory in MB for the lambda function used to perform garbage collection.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_BATCH_SIZE = Index.propertyBuilder("sleeper.gc.batch.size")
            .description("The size of the batch of files ready for garbage collection requested from the State Store.")
            .defaultValue("2000")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR).build();
    UserDefinedInstanceProperty DEFAULT_GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION = Index.propertyBuilder("sleeper.default.gc.delay.minutes")
            .description("A file will not be deleted until this number of minutes have passed after it has been marked as ready for " +
                    "garbage collection. The reason for not deleting files immediately after they have been marked as ready for " +
                    "garbage collection is that they may still be in use by queries. This property can be overridden on a per-table " +
                    "basis.")
            .defaultValue("15")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR).build();

    // Compaction
    UserDefinedInstanceProperty ECR_COMPACTION_REPO = Index.propertyBuilder("sleeper.compaction.repo")
            .description("The name of the repository for the compaction container. The Docker image from the compaction-job-execution module " +
                    "should have been uploaded to an ECR repository of this name in this account.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.queue.visibility.timeout.seconds")
            .description("The visibility timeout for the queue of compaction jobs.")
            .defaultValue("900")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.keepalive.period.seconds")
            .description("The frequency, in seconds, with which change message visibility requests are sent to extend the " +
                    "visibility of messages on the compaction job queue so that they are not processed by other processes.\n" +
                    "This should be less than the value of sleeper.compaction.queue.visibility.timeout.seconds.")
            .defaultValue("300")
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.compaction.job.creation.period.minutes")
            .description("The rate at which the compaction job creation lambda runs (in minutes, must be >=1).")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_JOB_CREATION_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.compaction.job.creation.memory")
            .description("The amount of memory for the lambda that creates compaction jobs.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.job.creation.timeout.seconds")
            .description("The timeout for the lambda that creates compaction jobs in seconds.")
            .defaultValue("900")
            .validationPredicate(Utils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty MAXIMUM_CONCURRENT_COMPACTION_TASKS = Index.propertyBuilder("sleeper.compaction.max.concurrent.tasks")
            .description("The maximum number of concurrent compaction tasks to run.")
            .defaultValue("300")
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.compaction.task.creation.period.minutes")
            .description("The rate at which a check to see if compaction ECS tasks need to be created is made (in minutes, must be >= 1).")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_TASK_CPU_ARCHITECTURE = Index.propertyBuilder("sleeper.compaction.task.cpu.architecture")
            .description("The CPU architecture to run compaction tasks on.\n" +
                    "See Task CPU architecture at https://docs.aws.amazon.com/AmazonECS/latest/developerguide/AWS_Fargate.html")
            .defaultValue("X86_64")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_TASK_ARM_CPU = Index.propertyBuilder("sleeper.compaction.task.arm.cpu")
            .description("The CPU for a compaction task using an ARM architecture.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_TASK_ARM_MEMORY = Index.propertyBuilder("sleeper.compaction.task.arm.memory")
            .description("The memory for a compaction task using an ARM architecture.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("4096")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_TASK_X86_CPU = Index.propertyBuilder("sleeper.compaction.task.x86.cpu")
            .description("The CPU for a compaction task using an x86 architecture.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_TASK_X86_MEMORY = Index.propertyBuilder("sleeper.compaction.task.x86.memory")
            .description("The memory for a compaction task using an x86 architecture.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("4096")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_ECS_LAUNCHTYPE = Index.propertyBuilder("sleeper.compaction.ecs.launch.type")
            .description("What launch type should compaction containers use? Valid options: FARGATE, EC2.")
            .defaultValue("FARGATE")
            .validationPredicate(Arrays.asList("EC2", "FARGATE")::contains)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    UserDefinedInstanceProperty COMPACTION_EC2_TYPE = Index.propertyBuilder("sleeper.compaction.ec2.type")
            .description("The EC2 instance type to use for compaction tasks (when using EC2-based compactions).")
            .defaultValue("t3.xlarge")
            .validationPredicate(Utils::isNonNullNonEmptyString)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    UserDefinedInstanceProperty COMPACTION_EC2_POOL_MINIMUM = Index.propertyBuilder("sleeper.compaction.ec2.pool.minimum")
            .description("The minimum number of instances for the EC2 cluster (when using EC2-based compactions).")
            .defaultValue("0")
            .validationPredicate(Utils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    UserDefinedInstanceProperty COMPACTION_EC2_POOL_DESIRED = Index.propertyBuilder("sleeper.compaction.ec2.pool.desired")
            .description("The initial desired number of instances for the EC2 cluster (when using EC2-based compactions).\n" +
                    "Can be set by dividing initial maximum containers by number that should fit on instance type.")
            .defaultValue("0")
            .validationPredicate(Utils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    UserDefinedInstanceProperty COMPACTION_EC2_POOL_MAXIMUM = Index.propertyBuilder("sleeper.compaction.ec2.pool.maximum")
            .description("The maximum number of instances for the EC2 cluster (when using EC2-based compactions).")
            .defaultValue("75")
            .validationPredicate(Utils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    UserDefinedInstanceProperty COMPACTION_EC2_ROOT_SIZE = Index.propertyBuilder("sleeper.compaction.ec2.root.size")
            .description("The size in GiB of the root EBS volume attached to the EC2 instances (when using EC2-based compactions).")
            .defaultValue("50")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    UserDefinedInstanceProperty COMPACTION_STATUS_STORE_ENABLED = Index.propertyBuilder("sleeper.compaction.status.store.enabled")
            .description("Flag to enable/disable storage of tracking information for compaction jobs and tasks.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_JOB_STATUS_TTL_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.job.status.ttl")
            .description("The time to live in seconds for compaction job updates in the status store. Default is 1 week.\n" +
                    "The expiry time is fixed when an update is saved to the store, so changing this will only affect new data.")
            .defaultValue("604800") // Default is 1 week
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_TASK_STATUS_TTL_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.task.status.ttl")
            .description("The time to live in seconds for compaction task updates in the status store. Default is 1 week.\n" +
                    "The expiry time is fixed when an update is saved to the store, so changing this will only affect new data.")
            .defaultValue("604800") // Default is 1 week
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty DEFAULT_COMPACTION_STRATEGY_CLASS = Index.propertyBuilder("sleeper.default.compaction.strategy.class")
            .description("The name of the class that defines how compaction jobs should be created. " +
                    "This should implement sleeper.compaction.strategy.CompactionStrategy. The value of this property is the " +
                    "default value which can be overridden on a per-table basis.")
            .defaultValue("sleeper.compaction.strategy.impl.SizeRatioCompactionStrategy")
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty DEFAULT_COMPACTION_FILES_BATCH_SIZE = Index.propertyBuilder("sleeper.default.compaction.files.batch.size")
            .description("The minimum number of files to read in a compaction job. Note that the state store " +
                    "must support atomic updates for this many files. For the DynamoDBStateStore this " +
                    "is 11. It can be overridden on a per-table basis.\n" +
                    "(NB This does not apply to splitting jobs which will run even if there is only 1 file.)\n" +
                    "This is a default value and will be used if not specified in the table.properties file.")
            .defaultValue("11")
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty DEFAULT_SIZERATIO_COMPACTION_STRATEGY_RATIO = Index.propertyBuilder("sleeper.default.table.compaction.strategy.sizeratio.ratio")
            .description("Used by the SizeRatioCompactionStrategy to decide if a group of files should be compacted.\n" +
                    "If the file sizes are s_1, ..., s_n then the files are compacted if s_1 + ... + s_{n-1} >= ratio * s_n.\n" +
                    "It can be overridden on a per-table basis.")
            .defaultValue("3")
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty DEFAULT_SIZERATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION = Index.propertyBuilder("sleeper.default.table.compaction.strategy.sizeratio.max.concurrent.jobs.per.partition")
            .description("Used by the SizeRatioCompactionStrategy to control the maximum number of jobs that can be running " +
                    "concurrently per partition. It can be overridden on a per-table basis.")
            .defaultValue("" + Integer.MAX_VALUE)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();

    // Query
    UserDefinedInstanceProperty MAXIMUM_CONNECTIONS_TO_S3_FOR_QUERIES = Index.propertyBuilder("sleeper.query.s3.max-connections")
            .description("The maximum number of simultaneous connections to S3 from a single query runner. This is separated " +
                    "from the main one as it's common for a query runner to need to open more files at once.")
            .defaultValue("1024")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_PROCESSOR_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.query.processor.memory")
            .description("The amount of memory in MB for the lambda that executes queries.")
            .defaultValue("2048")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.query.processor.timeout.seconds")
            .description("The timeout for the lambda that executes queries in seconds.")
            .defaultValue("900")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty QUERY_PROCESSING_LAMBDA_STATE_REFRESHING_PERIOD_IN_SECONDS = Index.propertyBuilder("sleeper.query.processor.state.refresh.period.seconds")
            .description("The frequency with which the query processing lambda refreshes its knowledge of the system state " +
                    "(i.e. the partitions and the mapping from partition to files), in seconds.")
            .defaultValue("60")
            .propertyGroup(InstancePropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_PROCESSING_LAMBDA_RESULTS_BATCH_SIZE = Index.propertyBuilder("sleeper.query.processor.results.batch.size")
            .description("The maximum number of records to include in a batch of query results send to " +
                    "the results queue from the query processing lambda.")
            .defaultValue("2000")
            .propertyGroup(InstancePropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_PROCESSOR_LAMBDA_RECORD_RETRIEVAL_THREADS = Index.propertyBuilder("sleeper.query.processor.record.retrieval.threads")
            .description("The size of the thread pool for retrieving records in a query processing lambda.")
            .defaultValue("10")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_TRACKER_ITEM_TTL_IN_DAYS = Index.propertyBuilder("sleeper.query.tracker.ttl.days")
            .description("This value is used to set the time-to-live on the tracking of the queries in the DynamoDB-based query tracker.")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_RESULTS_BUCKET_EXPIRY_IN_DAYS = Index.propertyBuilder("sleeper.query.results.bucket.expiry.days")
            .description("The length of time the results of queries remain in the query results bucket before being deleted.")
            .defaultValue("7")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.QUERY)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty DEFAULT_RESULTS_ROW_GROUP_SIZE = Index.propertyBuilder("sleeper.default.query.results.rowgroup.size")
            .description("The default value of the rowgroup size used when the results of queries are written to Parquet files. The " +
                    "value given below is 8MiB. This value can be overridden using the query config.")
            .defaultValue("" + (8 * 1024 * 1024)) // 8 MiB
            .propertyGroup(InstancePropertyGroup.QUERY).build();
    UserDefinedInstanceProperty DEFAULT_RESULTS_PAGE_SIZE = Index.propertyBuilder("sleeper.default.query.results.page.size")
            .description("The default value of the page size used when the results of queries are written to Parquet files. The " +
                    "value given below is 128KiB. This value can be overridden using the query config.")
            .defaultValue("" + (128 * 1024)) // 128 KiB
            .propertyGroup(InstancePropertyGroup.QUERY).build();

    // Dashboard
    UserDefinedInstanceProperty DASHBOARD_TIME_WINDOW_MINUTES = Index.propertyBuilder("sleeper.dashboard.time.window.minutes")
            .description("The period in minutes used in the dashboard.")
            .defaultValue("5")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.DASHBOARD)
            .runCDKDeployWhenChanged(true).build();

    // Logging levels
    UserDefinedInstanceProperty LOGGING_LEVEL = Index.propertyBuilder("sleeper.logging.level")
            .description("The logging level for logging Sleeper classes. This does not apply to the MetricsLogger which is always set to INFO.")
            .propertyGroup(InstancePropertyGroup.LOGGING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty APACHE_LOGGING_LEVEL = Index.propertyBuilder("sleeper.logging.apache.level")
            .description("The logging level for Apache logs that are not Parquet.")
            .propertyGroup(InstancePropertyGroup.LOGGING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty PARQUET_LOGGING_LEVEL = Index.propertyBuilder("sleeper.logging.parquet.level")
            .description("The logging level for Parquet logs.")
            .propertyGroup(InstancePropertyGroup.LOGGING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty AWS_LOGGING_LEVEL = Index.propertyBuilder("sleeper.logging.aws.level")
            .description("The logging level for AWS logs.")
            .propertyGroup(InstancePropertyGroup.LOGGING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty ROOT_LOGGING_LEVEL = Index.propertyBuilder("sleeper.logging.root.level")
            .description("The logging level for everything else.")
            .propertyGroup(InstancePropertyGroup.LOGGING)
            .runCDKDeployWhenChanged(true).build();

    // Athena
    UserDefinedInstanceProperty SPILL_BUCKET_AGE_OFF_IN_DAYS = Index.propertyBuilder("sleeper.athena.spill.bucket.ageoff.days")
            .description("The number of days before objects in the spill bucket are deleted.")
            .defaultValue("1")
            .propertyGroup(InstancePropertyGroup.ATHENA)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty ATHENA_COMPOSITE_HANDLER_CLASSES = Index.propertyBuilder("sleeper.athena.handler.classes")
            .description("The fully qualified composite classes to deploy. These are the classes that interact with Athena. " +
                    "You can choose to remove one if you don't need them. Both are deployed by default.")
            .defaultValue("sleeper.athena.composite.SimpleCompositeHandler,sleeper.athena.composite.IteratorApplyingCompositeHandler")
            .propertyGroup(InstancePropertyGroup.ATHENA)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty ATHENA_COMPOSITE_HANDLER_MEMORY = Index.propertyBuilder("sleeper.athena.handler.memory")
            .description("The amount of memory (GB) the athena composite handler has.")
            .defaultValue("4096")
            .propertyGroup(InstancePropertyGroup.ATHENA)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty ATHENA_COMPOSITE_HANDLER_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.athena.handler.timeout.seconds")
            .description("The timeout in seconds for the athena composite handler.")
            .defaultValue("900")
            .validationPredicate(Utils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.ATHENA)
            .runCDKDeployWhenChanged(true).build();

    // Default values
    UserDefinedInstanceProperty DEFAULT_S3A_READAHEAD_RANGE = Index.propertyBuilder("sleeper.default.fs.s3a.readahead.range")
            .description("The readahead range set on the Hadoop configuration when reading Parquet files in a query\n" +
                    "(see https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html).")
            .defaultValue("64K")
            .validationPredicate(Utils::isValidHadoopLongBytes)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_ROW_GROUP_SIZE = Index.propertyBuilder("sleeper.default.rowgroup.size")
            .description("The size of the row group in the Parquet files (default is 8MiB).")
            .defaultValue("" + (8 * 1024 * 1024)) // 8 MiB
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_PAGE_SIZE = Index.propertyBuilder("sleeper.default.page.size")
            .description("The size of the pages in the Parquet files (default is 128KiB).")
            .defaultValue("" + (128 * 1024)) // 128 KiB
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_COMPRESSION_CODEC = Index.propertyBuilder("sleeper.default.compression.codec")
            .description("The compression codec to use in the Parquet files.\n" +
                    "Valid values are: " + describeEnumValuesInLowerCase(CompressionCodec.class))
            .defaultValue("zstd")
            .validationPredicate(Utils::isValidCompressionCodec)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS = Index.propertyBuilder("sleeper.default.parquet.dictionary.encoding.rowkey.fields")
            .description("Whether dictionary encoding should be used for row key columns in the Parquet files.")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS = Index.propertyBuilder("sleeper.default.parquet.dictionary.encoding.sortkey.fields")
            .description("Whether dictionary encoding should be used for sort key columns in the Parquet files.")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_DICTIONARY_ENCODING_FOR_VALUE_FIELDS = Index.propertyBuilder("sleeper.default.parquet.dictionary.encoding.value.fields")
            .description("Whether dictionary encoding should be used for value columns in the Parquet files.")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH = Index.propertyBuilder("sleeper.default.parquet.columnindex.truncate.length")
            .description("Used to set parquet.columnindex.truncate.length, see documentation here:\n" +
                    "https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/README.md\n" +
                    "The length in bytes to truncate binary values in a column index.")
            .defaultValue("128")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_STATISTICS_TRUNCATE_LENGTH = Index.propertyBuilder("sleeper.default.parquet.statistics.truncate.length")
            .description("Used to set parquet.statistics.truncate.length, see documentation here:\n" +
                    "https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/README.md\n" +
                    "The length in bytes to truncate the min/max binary values in row groups.")
            .defaultValue("2147483647")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY_ENABLED = Index.propertyBuilder("sleeper.default.table.dynamo.pointintimerecovery")
            .description("This specifies whether point in time recovery is turned on for DynamoDB tables. This default can " +
                    "be overridden by a table property.")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.DEFAULT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty DEFAULT_DYNAMO_STRONGLY_CONSISTENT_READS = Index.propertyBuilder("sleeper.default.table.dynamo.strongly.consistent.reads")
            .description("This specifies whether queries and scans against DynamoDB tables used in the DynamoDB state store " +
                    "are strongly consistent. This default can be overridden by a table property.")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT = Index.propertyBuilder("sleeper.default.bulk.import.min.leaf.partitions")
            .description("Specifies the minimum number of leaf partitions that are needed to run a bulk import job. " +
                    "If this minimum has not been reached, bulk import jobs will refuse to start.")
            .defaultValue("64")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();

    UserDefinedInstanceProperty DEFAULT_INGEST_BATCHER_MIN_JOB_SIZE = Index.propertyBuilder("sleeper.default.ingest.batcher.job.min.size")
            .description("Specifies the minimum total file size required for an ingest job to be batched and sent. " +
                    "An ingest job will be created if the batcher runs while this much data is waiting, and the " +
                    "minimum number of files is also met.")
            .defaultValue("1G")
            .validationPredicate(Utils::isValidNumberOfBytes)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_INGEST_BATCHER_MAX_JOB_SIZE = Index.propertyBuilder("sleeper.default.ingest.batcher.job.max.size")
            .description("Specifies the maximum total file size for a job in the ingest batcher. " +
                    "If more data is waiting than this, it will be split into multiple jobs. " +
                    "If a single file exceeds this, it will still be ingested in its own job. " +
                    "It's also possible some data may be left for a future run of the batcher if some recent files " +
                    "overflow the size of a job but aren't enough to create a job on their own.")
            .defaultValue("5G")
            .validationPredicate(Utils::isValidNumberOfBytes)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_INGEST_BATCHER_MIN_JOB_FILES = Index.propertyBuilder("sleeper.default.ingest.batcher.job.min.files")
            .description("Specifies the minimum number of files for a job in the ingest batcher. " +
                    "An ingest job will be created if the batcher runs while this many files are waiting, and the " +
                    "minimum size of files is also met.")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_INGEST_BATCHER_MAX_JOB_FILES = Index.propertyBuilder("sleeper.default.ingest.batcher.job.max.files")
            .description("Specifies the maximum number of files for a job in the ingest batcher. " +
                    "If more files are waiting than this, they will be split into multiple jobs. " +
                    "It's possible some data may be left for a future run of the batcher if some recent files " +
                    "overflow the size of a job but aren't enough to create a job on their own.")
            .defaultValue("100")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_INGEST_BATCHER_MAX_FILE_AGE_SECONDS = Index.propertyBuilder("sleeper.default.ingest.batcher.file.max.age.seconds")
            .description("Specifies the maximum time in seconds that a file can be held in the batcher before it " +
                    "will be included in an ingest job. When any file has been waiting for longer than this, jobs " +
                    "will be created for all the currently held files, even if other criteria for a batch are not " +
                    "met.")
            .defaultValue("300")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_INGEST_BATCHER_INGEST_MODE = Index.propertyBuilder("sleeper.default.ingest.batcher.ingest.mode")
            .description("Specifies the target ingest queue where batched jobs are sent.\n" +
                    "Valid values are: " + describeEnumValuesInLowerCase(BatchIngestMode.class))
            .defaultValue(BatchIngestMode.STANDARD_INGEST.name().toLowerCase(Locale.ROOT))
            .validationPredicate(BatchIngestMode::isValidMode)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_INGEST_BATCHER_TRACKING_TTL_MINUTES = Index.propertyBuilder("sleeper.default.ingest.batcher.file.tracking.ttl.minutes")
            .description("The time in minutes that the tracking information is retained for a file before the " +
                    "records of its ingest are deleted (eg. which ingest job it was assigned to, the time this " +
                    "occurred, the size of the file).\n" +
                    "The expiry time is fixed when a file is saved to the store, so changing this will only affect " +
                    "new data.\n" +
                    "Defaults to 1 week.")
            .defaultValue("" + 60 * 24 * 7)
            .validationPredicate(Utils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();

    static List<UserDefinedInstanceProperty> getAll() {
        return Index.INSTANCE.getAll();
    }

    static boolean has(String propertyName) {
        return Index.INSTANCE.getByName(propertyName).isPresent();
    }

    class Index {
        private Index() {
        }

        private static final SleeperPropertyIndex<UserDefinedInstanceProperty> INSTANCE = new SleeperPropertyIndex<>();

        private static UserDefinedInstancePropertyImpl.Builder propertyBuilder(String propertyName) {
            return UserDefinedInstancePropertyImpl.named(propertyName)
                    .addToIndex(INSTANCE::add);
        }
    }
}
