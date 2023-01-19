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
            .validationPredicate(Objects::nonNull).build();
    // Common
    UserDefinedInstanceProperty ID = named("sleeper.id")
            .validationPredicate(Objects::nonNull).build();
    UserDefinedInstanceProperty JARS_BUCKET = named("sleeper.jars.bucket")
            .validationPredicate(Objects::nonNull).build();
    UserDefinedInstanceProperty USER_JARS = named("sleeper.userjars").build();
    UserDefinedInstanceProperty TAGS_FILE = named("sleeper.tags.file").build();
    UserDefinedInstanceProperty TAGS = named("sleeper.tags").build();
    UserDefinedInstanceProperty STACK_TAG_NAME = named("sleeper.stack.tag.name")
            .defaultValue("DeploymentStack").build();
    UserDefinedInstanceProperty RETAIN_INFRA_AFTER_DESTROY = named("sleeper.retain.infra.after.destroy")
            .defaultValue("true")
            .validationPredicate(Utils::isTrueOrFalse).build();
    UserDefinedInstanceProperty OPTIONAL_STACKS = named("sleeper.optional.stacks")
            .defaultValue("CompactionStack,GarbageCollectorStack,IngestStack,PartitionSplittingStack,QueryStack,AthenaStack,EmrBulkImportStack,DashboardStack").build();
    UserDefinedInstanceProperty ACCOUNT = named("sleeper.account")
            .validationPredicate(Objects::nonNull).build();
    UserDefinedInstanceProperty REGION = named("sleeper.region")
            .validationPredicate(Objects::nonNull).build();
    UserDefinedInstanceProperty VERSION = named("sleeper.version")
            .validationPredicate(Objects::nonNull).build();
    UserDefinedInstanceProperty VPC_ID = named("sleeper.vpc")
            .validationPredicate(Objects::nonNull).build();
    UserDefinedInstanceProperty VPC_ENDPOINT_CHECK = named("sleeper.vpc.endpoint.check")
            .defaultValue("true").build();
    UserDefinedInstanceProperty SUBNET = named("sleeper.subnet")
            .validationPredicate(Objects::nonNull).build();
    UserDefinedInstanceProperty FILE_SYSTEM = named("sleeper.filesystem")
            .defaultValue("s3a://").build();
    UserDefinedInstanceProperty EMAIL_ADDRESS_FOR_ERROR_NOTIFICATION = named("sleeper.errors.email").build();
    UserDefinedInstanceProperty QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS = named("sleeper.queue.visibility.timeout.seconds")
            .defaultValue("900")
            .validationPredicate(Utils::isValidLambdaTimeout).build();
    UserDefinedInstanceProperty LOG_RETENTION_IN_DAYS = named("sleeper.log.retention.days")
            .defaultValue("30")
            .validationPredicate(Utils::isValidLogRetention).build();
    UserDefinedInstanceProperty MAXIMUM_CONNECTIONS_TO_S3 = named("sleeper.s3.max-connections")
            .defaultValue("25")
            .validationPredicate(Utils::isPositiveInteger).build();
    UserDefinedInstanceProperty FARGATE_VERSION = named("sleeper.fargate.version")
            .defaultValue("1.4.0").build();
    UserDefinedInstanceProperty TASK_RUNNER_LAMBDA_MEMORY_IN_MB = named("sleeper.task.runner.memory")
            .defaultValue("1024").build();
    UserDefinedInstanceProperty TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS = named("sleeper.task.runner.timeout.seconds")
            .defaultValue("900")
            .validationPredicate(Utils::isValidLambdaTimeout).build();
    UserDefinedInstanceProperty METRICS_NAMESPACE = named("sleeper.metrics.namespace")
            .defaultValue("Sleeper")
            .validationPredicate(Utils::isNonNullNonEmptyString).build();

    // Ingest
    UserDefinedInstanceProperty ECR_INGEST_REPO = named("sleeper.ingest.repo").build();
    UserDefinedInstanceProperty MAXIMUM_CONCURRENT_INGEST_TASKS = named("sleeper.ingest.max.concurrent.tasks")
            .defaultValue("200").build();
    UserDefinedInstanceProperty INGEST_TASK_CREATION_PERIOD_IN_MINUTES = named("sleeper.ingest.task.creation.period.minutes")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveInteger).build();
    UserDefinedInstanceProperty INGEST_KEEP_ALIVE_PERIOD_IN_SECONDS = named("sleeper.ingest.keepalive.period.seconds")
            .defaultValue("300").build();
    UserDefinedInstanceProperty S3A_INPUT_FADVISE = named("sleeper.ingest.fs.s3a.experimental.input.fadvise")
            .defaultValue("sequential")
            .validationPredicate(Utils::isValidFadvise).build();
    UserDefinedInstanceProperty INGEST_TASK_CPU = named("sleeper.ingest.task.cpu")
            .defaultValue("2048").build();
    UserDefinedInstanceProperty INGEST_TASK_MEMORY = named("sleeper.ingest.task.memory")
            .defaultValue("4096").build();
    UserDefinedInstanceProperty INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS = named("sleeper.ingest.partition.refresh.period")
            .defaultValue("120").build();
    UserDefinedInstanceProperty INGEST_SOURCE_BUCKET = named("sleeper.ingest.source.bucket").build();
    UserDefinedInstanceProperty INGEST_RECORD_BATCH_TYPE = named("sleeper.ingest.record.batch.type")
            .defaultValue("arraylist").build();
    UserDefinedInstanceProperty INGEST_PARTITION_FILE_WRITER_TYPE = named("sleeper.ingest.partition.file.writer.type")
            .defaultValue("direct").build();

    // ArrayList ingest
    UserDefinedInstanceProperty MAX_RECORDS_TO_WRITE_LOCALLY = named("sleeper.ingest.max.local.records")
            .defaultValue("100000000").build();
    UserDefinedInstanceProperty MAX_IN_MEMORY_BATCH_SIZE = named("sleeper.ingest.memory.max.batch.size")
            .defaultValue("1000000").build();

    // Arrow ingest
    UserDefinedInstanceProperty ARROW_INGEST_WORKING_BUFFER_BYTES = named("sleeper.ingest.arrow.working.buffer.bytes")
            .defaultValue("268435456").build();                    // 256M
    UserDefinedInstanceProperty ARROW_INGEST_BATCH_BUFFER_BYTES = named("sleeper.ingest.arrow.batch.buffer.bytes")
            .defaultValue("1073741824").build();                       // 1G
    UserDefinedInstanceProperty ARROW_INGEST_MAX_LOCAL_STORE_BYTES = named("sleeper.ingest.arrow.max.local.store.bytes")
            .defaultValue("2147483648").build();                 // 2G
    UserDefinedInstanceProperty ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS = named("sleeper.ingest.arrow.max.single.write.to.file.records")
            .defaultValue("1024").build(); // 1K

    // Async ingest partition file writer
    UserDefinedInstanceProperty ASYNC_INGEST_CLIENT_TYPE = named("sleeper.ingest.async.client.type")
            .defaultValue("crt").build(); // crt or java
    UserDefinedInstanceProperty ASYNC_INGEST_CRT_PART_SIZE_BYTES = named("sleeper.ingest.async.crt.part.size.bytes")
            .defaultValue("134217728")
            .validationPredicate(Utils::isPositiveLong).build(); // 128M
    UserDefinedInstanceProperty ASYNC_INGEST_CRT_TARGET_THROUGHPUT_GBPS = named("sleeper.ingest.async.crt.target.throughput.gbps")
            .defaultValue("10")
            .validationPredicate(Utils::isPositiveDouble).build();

    // Status Store
    UserDefinedInstanceProperty INGEST_STATUS_STORE_ENABLED = named("sleeper.ingest.status.store.enabled")
            .defaultValue("true").build();
    UserDefinedInstanceProperty INGEST_JOB_STATUS_TTL_IN_SECONDS = named("sleeper.ingest.job.status.ttl")
            .defaultValue("604800")
            .validationPredicate(Utils::isPositiveInteger).build(); // Default is 1 week

    UserDefinedInstanceProperty INGEST_TASK_STATUS_TTL_IN_SECONDS = named("sleeper.ingest.task.status.ttl")
            .defaultValue("604800")
            .validationPredicate(Utils::isPositiveInteger).build(); // Default is 1 week

    // Bulk Import - properties that are applicable to all bulk import platforms
    UserDefinedInstanceProperty BULK_IMPORT_CLASS_NAME = named("sleeper.bulk.import.class.name")
            .defaultValue("sleeper.bulkimport.job.runner.dataframelocalsort.BulkImportDataframeLocalSortRunner").build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_SHUFFLE_MAPSTATUS_COMPRESSION_CODEC = named("sleeper.bulk.import.emr.spark.shuffle.mapStatus.compression.codec")
            .defaultValue("lz4").build(); // Stops "Decompression error: Version not supported" errors - only a value of "lz4" has been tested. This is used to set the value of spark.shuffle.mapStatus.compression.codec on the Spark configuration.
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_SPECULATION = named("sleeper.bulk.import.emr.spark.speculation")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse).build();
    // This is used to set the value of spark.speculation on the Spark configuration.
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_SPECULATION_QUANTILE = named("sleeper.bulk.import.spark.speculation.quantile")
            .defaultValue("0.75").build(); // This is used to set the value of spark.speculation.quantile on the Spark configuration.

    // Bulk import using EMR - these properties are used by both the persistent
    // and non-persistent EMR stacks
    UserDefinedInstanceProperty BULK_IMPORT_EMR_EC2_KEYPAIR_NAME = named("sleeper.bulk.import.emr.keypair.name").build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP = named("sleeper.bulk.import.emr.master.additional.security.group").build();
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
            .defaultValue("16g").build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_DRIVER_MEMORY = named("sleeper.bulk.import.emr.spark.driver.memory")
            .defaultValue(BULK_IMPORT_EMR_SPARK_EXECUTOR_MEMORY.getDefaultValue()).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_EXECUTOR_INSTANCES = named("sleeper.bulk.import.emr.spark.executor.instances")
            .defaultValue("29").build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD = named("sleeper.bulk.import.emr.spark.yarn.executor.memory.overhead")
            .defaultValue("2g").build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_YARN_DRIVER_MEMORY_OVERHEAD = named("sleeper.bulk.import.emr.spark.yarn.driver.memory.overhead")
            .defaultValue(BULK_IMPORT_EMR_SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD.getDefaultValue()).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_DEFAULT_PARALLELISM = named("sleeper.bulk.import.emr.spark.default.parallelism")
            .defaultValue("290").build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_SQL_SHUFFLE_PARTITIONS = named("sleeper.bulk.import.emr.spark.sql.shuffle.partitions")
            .defaultValue(BULK_IMPORT_EMR_SPARK_DEFAULT_PARALLELISM.getDefaultValue()).build();
    //  - Properties that are independent of the instance type and number of instances:
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_EXECUTOR_CORES = named("sleeper.bulk.import.emr.spark.executor.cores")
            .defaultValue("5").build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_DRIVER_CORES = named("sleeper.bulk.import.emr.spark.driver.cores")
            .defaultValue(BULK_IMPORT_EMR_SPARK_EXECUTOR_CORES.getDefaultValue()).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_NETWORK_TIMEOUT = named("sleeper.bulk.import.emr.spark.network.timeout")
            .defaultValue("800s").build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_EXECUTOR_HEARTBEAT_INTERVAL = named("sleeper.bulk.import.emr.spark.executor.heartbeat.interval")
            .defaultValue("60s").build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_DYNAMIC_ALLOCATION_ENABLED = named("sleeper.bulk.import.emr.spark.dynamic.allocation.enabled")
            .defaultValue("false").build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_MEMORY_FRACTION = named("sleeper.bulk.import.emr.spark.memory.fraction")
            .defaultValue("0.80").build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_MEMORY_STORAGE_FRACTION = named("sleeper.bulk.import.emr.spark.memory.storage.fraction")
            .defaultValue("0.30").build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS = named("sleeper.bulk.import.emr.spark.executor.extra.java.options")
            .defaultValue("-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'").build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_DRIVER_EXTRA_JAVA_OPTIONS = named("sleeper.bulk.import.emr.spark.driver.extra.java.options")
            .defaultValue(BULK_IMPORT_EMR_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS.getDefaultValue()).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_YARN_SCHEDULER_REPORTER_THREAD_MAX_FAILURES = named("sleeper.bulk.import.emr.spark.yarn.scheduler.reporter.thread.max.failures")
            .defaultValue("5").build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_STORAGE_LEVEL = named("sleeper.bulk.import.emr.spark.storage.level")
            .defaultValue("MEMORY_AND_DISK_SER").build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_RDD_COMPRESS = named("sleeper.bulk.import.emr.spark.rdd.compress")
            .defaultValue("true").build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_SHUFFLE_COMPRESS = named("sleeper.bulk.import.emr.spark.shuffle.compress")
            .defaultValue("true").build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_SHUFFLE_SPILL_COMPRESS = named("sleeper.bulk.import.emr.spark.shuffle.spill.compress")
            .defaultValue("true").build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_EBS_VOLUME_SIZE_IN_GB = named("sleeper.bulk.import.emr.ebs.volume.size.gb")
            .defaultValue("256")
            .validationPredicate(Utils::isValidEbsSize).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_EBS_VOLUME_TYPE = named("sleeper.bulk.import.emr.ebs.volume.type")
            .defaultValue("gp2")
            .validationPredicate(Utils::isValidEbsVolumeType).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_EBS_VOLUMES_PER_INSTANCE = named("sleeper.bulk.import.emr.ebs.volumes.per.instance")
            .defaultValue("4").validationPredicate(s -> Utils.isPositiveIntLtEqValue(s, 25)).build();

    // Bulk import using the non-persistent EMR approach
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL = named("sleeper.default.bulk.import.emr.release.label")
            .defaultValue("emr-6.9.0").build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE = named("sleeper.default.bulk.import.emr.master.instance.type")
            .defaultValue("m5.xlarge").build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE = named("sleeper.default.bulk.import.emr.executor.market.type")
            .defaultValue("SPOT").validationPredicate(s -> ("SPOT".equals(s) || "ON_DEMAND".equals(s))).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE = named("sleeper.default.bulk.import.emr.executor.instance.type")
            .defaultValue("m5.4xlarge").build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS = named("sleeper.default.bulk.import.emr.executor.initial.instances")
            .defaultValue("2").build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS = named("sleeper.default.bulk.import.emr.executor.max.instances")
            .defaultValue("10").build();

    // Bulk import using a persistent EMR cluster
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_RELEASE_LABEL = named("sleeper.bulk.import.persistent.emr.release.label")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL.getDefaultValue()).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MASTER_INSTANCE_TYPE = named("sleeper.bulk.import.persistent.emr.master.instance.type")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE.getDefaultValue()).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_INSTANCE_TYPE = named("sleeper.bulk.import.persistent.emr.core.instance.type")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE.getDefaultValue()).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING = named("sleeper.bulk.import.persistent.emr.use.managed.scaling")
            .defaultValue("true").build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MIN_NUMBER_OF_INSTANCES = named("sleeper.bulk.import.persistent.emr.min.instances")
            .defaultValue("1").build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MAX_NUMBER_OF_INSTANCES = named("sleeper.bulk.import.persistent.emr.max.instances")
            .defaultValue("10").build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_STEP_CONCURRENCY_LEVEL = named("sleeper.bulk.import.persistent.emr.step.concurrency.level")
            .defaultValue("2").build();

    // Bulk import using EKS
    UserDefinedInstanceProperty BULK_IMPORT_REPO = named("sleeper.bulk.import.eks.repo").build();

    // Partition splitting
    UserDefinedInstanceProperty PARTITION_SPLITTING_PERIOD_IN_MINUTES = named("sleeper.partition.splitting.period.minutes")
            .defaultValue("2").build();
    UserDefinedInstanceProperty MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB = named("sleeper.partition.splitting.files.maximum")
            .defaultValue("50").build();
    UserDefinedInstanceProperty FIND_PARTITIONS_TO_SPLIT_LAMBDA_MEMORY_IN_MB = named("sleeper.partition.splitting.finder.memory")
            .defaultValue("2048").build();
    UserDefinedInstanceProperty FIND_PARTITIONS_TO_SPLIT_TIMEOUT_IN_SECONDS = named("sleeper.partition.splitting.finder.timeout.seconds")
            .defaultValue("900").build();
    UserDefinedInstanceProperty SPLIT_PARTITIONS_LAMBDA_MEMORY_IN_MB = named("sleeper.partition.splitting.memory")
            .defaultValue("2048").build();
    UserDefinedInstanceProperty SPLIT_PARTITIONS_TIMEOUT_IN_SECONDS = named("sleeper.partition.splitting.timeout.seconds")
            .defaultValue("900").build();
    UserDefinedInstanceProperty DEFAULT_PARTITION_SPLIT_THRESHOLD = named("sleeper.default.partition.splitting.threshold")
            .defaultValue("1000000000").build();

    // Garbage collection
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_PERIOD_IN_MINUTES = named("sleeper.gc.period.minutes")
            .defaultValue("15").build();
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_LAMBDA_MEMORY_IN_MB = named("sleeper.gc.memory")
            .defaultValue("1024").build();
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_BATCH_SIZE = named("sleeper.gc.batch.size")
            .defaultValue("2000").build();
    UserDefinedInstanceProperty DEFAULT_GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION = named("sleeper.default.gc.delay.seconds")
            .defaultValue("600").build();

    // Compaction
    UserDefinedInstanceProperty ECR_COMPACTION_REPO = named("sleeper.compaction.repo").build();
    UserDefinedInstanceProperty COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS = named("sleeper.compaction.queue.visibility.timeout.seconds")
            .defaultValue("900").build();
    UserDefinedInstanceProperty COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS = named("sleeper.compaction.keepalive.period.seconds")
            .defaultValue("300").build();
    UserDefinedInstanceProperty COMPACTION_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES = named("sleeper.compaction.job.creation.period.minutes")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveInteger).build();
    UserDefinedInstanceProperty COMPACTION_JOB_CREATION_LAMBDA_MEMORY_IN_MB = named("sleeper.compaction.job.creation.memory")
            .defaultValue("1024").build();
    UserDefinedInstanceProperty COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS = named("sleeper.compaction.job.creation.timeout.seconds")
            .defaultValue("900")
            .validationPredicate(Utils::isValidLambdaTimeout).build();
    UserDefinedInstanceProperty MAXIMUM_CONCURRENT_COMPACTION_TASKS = named("sleeper.compaction.max.concurrent.tasks")
            .defaultValue("300").build();
    UserDefinedInstanceProperty COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES = named("sleeper.compaction.task.creation.period.minutes")
            .defaultValue("1").build(); // >0
    UserDefinedInstanceProperty COMPACTION_TASK_CPU_ARCHITECTURE = named("sleeper.compaction.task.cpu.architecture")
            .defaultValue("X86_64").build();
    UserDefinedInstanceProperty COMPACTION_TASK_ARM_CPU = named("sleeper.compaction.task.arm.cpu")
            .defaultValue("1024").build();
    UserDefinedInstanceProperty COMPACTION_TASK_ARM_MEMORY = named("sleeper.compaction.task.arm.memory")
            .defaultValue("4096").build();
    UserDefinedInstanceProperty COMPACTION_TASK_X86_CPU = named("sleeper.compaction.task.x86.cpu")
            .defaultValue("1024").build();
    UserDefinedInstanceProperty COMPACTION_TASK_X86_MEMORY = named("sleeper.compaction.task.x86.memory")
            .defaultValue("4096").build();
    UserDefinedInstanceProperty COMPACTION_STATUS_STORE_ENABLED = named("sleeper.compaction.status.store.enabled")
            .defaultValue("true").build();
    UserDefinedInstanceProperty COMPACTION_JOB_STATUS_TTL_IN_SECONDS = named("sleeper.compaction.job.status.ttl")
            .defaultValue("604800")
            .validationPredicate(Utils::isPositiveInteger).build(); // Default is 1 week
    UserDefinedInstanceProperty COMPACTION_TASK_STATUS_TTL_IN_SECONDS = named("sleeper.compaction.task.status.ttl")
            .defaultValue("604800")
            .validationPredicate(Utils::isPositiveInteger).build(); // Default is 1 week
    UserDefinedInstanceProperty DEFAULT_COMPACTION_STRATEGY_CLASS = named("sleeper.default.compaction.strategy.class")
            .defaultValue("sleeper.compaction.strategy.impl.SizeRatioCompactionStrategy").build();
    UserDefinedInstanceProperty DEFAULT_COMPACTION_FILES_BATCH_SIZE = named("sleeper.default.compaction.files.batch.size")
            .defaultValue("11").build();
    UserDefinedInstanceProperty DEFAULT_SIZERATIO_COMPACTION_STRATEGY_RATIO = named("sleeper.default.table.compaction.strategy.sizeratio.ratio")
            .defaultValue("3").build();
    UserDefinedInstanceProperty DEFAULT_SIZERATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION = named("sleeper.default.table.compaction.strategy.sizeratio.max.concurrent.jobs.per.partition")
            .defaultValue("" + Integer.MAX_VALUE).build();

    // Query
    UserDefinedInstanceProperty MAXIMUM_CONNECTIONS_TO_S3_FOR_QUERIES = named("sleeper.query.s3.max-connections")
            .defaultValue("1024")
            .validationPredicate(Utils::isPositiveInteger).build();
    UserDefinedInstanceProperty QUERY_PROCESSOR_LAMBDA_MEMORY_IN_MB = named("sleeper.query.processor.memory")
            .defaultValue("2048").build();
    UserDefinedInstanceProperty QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS = named("sleeper.query.processor.timeout.seconds")
            .defaultValue("900").build();
    UserDefinedInstanceProperty QUERY_PROCESSING_LAMBDA_STATE_REFRESHING_PERIOD_IN_SECONDS = named("sleeper.query.processor.state.refresh.period.seconds")
            .defaultValue("60").build();
    UserDefinedInstanceProperty QUERY_PROCESSING_LAMBDA_RESULTS_BATCH_SIZE = named("sleeper.query.processor.results.batch.size")
            .defaultValue("2000").build();
    UserDefinedInstanceProperty QUERY_PROCESSOR_LAMBDA_RECORD_RETRIEVAL_THREADS = named("sleeper.query.processor.record.retrieval.threads")
            .defaultValue("10")
            .validationPredicate(Utils::isPositiveInteger).build();
    UserDefinedInstanceProperty QUERY_TRACKER_ITEM_TTL_IN_DAYS = named("sleeper.query.tracker.ttl.days")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveInteger).build();
    UserDefinedInstanceProperty QUERY_RESULTS_BUCKET_EXPIRY_IN_DAYS = named("sleeper.query.results.bucket.expiry.days")
            .defaultValue("7")
            .validationPredicate(Utils::isPositiveInteger).build();
    UserDefinedInstanceProperty DEFAULT_RESULTS_ROW_GROUP_SIZE = named("sleeper.default.query.results.rowgroup.size")
            .defaultValue("" + (8 * 1024 * 1024)).build(); // 8 MiB
    UserDefinedInstanceProperty DEFAULT_RESULTS_PAGE_SIZE = named("sleeper.default.query.results.page.size")
            .defaultValue("" + (128 * 1024)).build(); // 128 KiB

    // Dashboard
    UserDefinedInstanceProperty DASHBOARD_TIME_WINDOW_MINUTES = named("sleeper.dashboard.time.window.minutes")
            .defaultValue("5")
            .validationPredicate(Utils::isPositiveInteger).build();

    // Logging levels
    UserDefinedInstanceProperty LOGGING_LEVEL = named("sleeper.logging.level").build();
    UserDefinedInstanceProperty APACHE_LOGGING_LEVEL = named("sleeper.logging.apache.level").build();
    UserDefinedInstanceProperty PARQUET_LOGGING_LEVEL = named("sleeper.logging.parquet.level").build();
    UserDefinedInstanceProperty AWS_LOGGING_LEVEL = named("sleeper.logging.aws.level").build();
    UserDefinedInstanceProperty ROOT_LOGGING_LEVEL = named("sleeper.logging.root.level").build();

    // Athena
    UserDefinedInstanceProperty SPILL_BUCKET_AGE_OFF_IN_DAYS = named("sleeper.athena.spill.bucket.ageoff.days")
            .defaultValue("1").build();
    UserDefinedInstanceProperty ATHENA_COMPOSITE_HANDLER_CLASSES = named("sleeper.athena.handler.classes")
            .defaultValue("sleeper.athena.composite.SimpleCompositeHandler,sleeper.athena.composite.IteratorApplyingCompositeHandler").build();
    UserDefinedInstanceProperty ATHENA_COMPOSITE_HANDLER_MEMORY = named("sleeper.athena.handler.memory")
            .defaultValue("4096").build();
    UserDefinedInstanceProperty ATHENA_COMPOSITE_HANDLER_TIMEOUT_IN_SECONDS = named("sleeper.athena.handler.timeout.seconds")
            .defaultValue("900")
            .validationPredicate(Utils::isValidLambdaTimeout).build();

    // Default values
    UserDefinedInstanceProperty DEFAULT_S3A_READAHEAD_RANGE = named("sleeper.default.fs.s3a.readahead.range")
            .defaultValue("64K").build();
    UserDefinedInstanceProperty DEFAULT_ROW_GROUP_SIZE = named("sleeper.default.rowgroup.size")
            .defaultValue("" + (8 * 1024 * 1024)).build(); // 8 MiB
    UserDefinedInstanceProperty DEFAULT_PAGE_SIZE = named("sleeper.default.page.size")
            .defaultValue("" + (128 * 1024)).build(); // 128 KiB
    UserDefinedInstanceProperty DEFAULT_COMPRESSION_CODEC = named("sleeper.default.compression.codec")
            .defaultValue("ZSTD")
            .validationPredicate(Utils::isValidCompressionCodec).build();
    UserDefinedInstanceProperty DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY_ENABLED = named("sleeper.default.table.dynamo.pointintimerecovery")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse).build();
    UserDefinedInstanceProperty DEFAULT_DYNAMO_STRONGLY_CONSISTENT_READS = named("sleeper.default.table.dynamo.strongly.consistent.reads")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse).build();

    static UserDefinedInstanceProperty[] values() {
        return UserDefinedInstancePropertyImpl.all().toArray(new UserDefinedInstanceProperty[0]);
    }
}
