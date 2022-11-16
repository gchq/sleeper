/*
 * Copyright 2022 Crown Copyright
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

import sleeper.configuration.Utils;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Sleeper properties set by the user. All non-mandatory properties should be accompanied by a default value and should
 * have a validation predicate for determining if the value a user has provided is valid. By default the predicate
 * always returns true indicating the property is valid.
 */
public enum UserDefinedInstanceProperty implements InstanceProperty {
    // Tables
    TABLE_PROPERTIES("sleeper.table.properties", Objects::nonNull),

    // Common
    ID("sleeper.id", Objects::nonNull),
    JARS_BUCKET("sleeper.jars.bucket", Objects::nonNull),
    USER_JARS("sleeper.userjars"),
    TAGS_FILE("sleeper.tags.file"),
    TAGS("sleeper.tags"),
    STACK_TAG_NAME("sleeper.stack.tag.name", "DeploymentStack"),
    RETAIN_INFRA_AFTER_DESTROY("sleeper.retain.infra.after.destroy", "true", Utils::isTrueOrFalse),
    OPTIONAL_STACKS("sleeper.optional.stacks",
            "CompactionStack,GarbageCollectorStack,IngestStack,PartitionSplittingStack,QueryStack,AthenaStack,EmrBulkImportStack,DashboardStack"),
    ACCOUNT("sleeper.account", Objects::nonNull),
    REGION("sleeper.region", Objects::nonNull),
    VERSION("sleeper.version", Objects::nonNull),
    VPC_ID("sleeper.vpc", Objects::nonNull),
    VPC_ENDPOINT_CHECK("sleeper.vpc.endpoint.check", "true"),
    SUBNET("sleeper.subnet", Objects::nonNull),
    FILE_SYSTEM("sleeper.filesystem", "s3a://"),
    EMAIL_ADDRESS_FOR_ERROR_NOTIFICATION("sleeper.errors.email"),
    QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS("sleeper.queue.visibility.timeout.seconds", "900", Utils::isValidLambdaTimeout),
    LOG_RETENTION_IN_DAYS("sleeper.log.retention.days", "30", Utils::isValidLogRetention),
    MAXIMUM_CONNECTIONS_TO_S3("sleeper.s3.max-connections", "25", Utils::isPositiveInteger),
    FARGATE_VERSION("sleeper.fargate.version", "1.4.0"),
    TASK_RUNNER_LAMBDA_MEMORY_IN_MB("sleeper.task.runner.memory", "1024"),
    TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS("sleeper.task.runner.timeout.seconds", "900", Utils::isValidLambdaTimeout),
    METRICS_NAMESPACE("sleeper.metrics.namespace", "Sleeper", Utils::isNonNullNonEmptyString),

    // Ingest
    ECR_INGEST_REPO("sleeper.ingest.repo"),
    MAXIMUM_CONCURRENT_INGEST_TASKS("sleeper.ingest.max.concurrent.tasks", "200"),
    INGEST_TASK_CREATION_PERIOD_IN_MINUTES("sleeper.ingest.task.creation.period.minutes", "1", Utils::isPositiveInteger),
    INGEST_KEEP_ALIVE_PERIOD_IN_SECONDS("sleeper.ingest.keepalive.period.seconds", "300"),
    S3A_INPUT_FADVISE("sleeper.ingest.fs.s3a.experimental.input.fadvise", "sequential", Utils::isValidFadvise),
    INGEST_TASK_CPU("sleeper.ingest.task.cpu", "2048"),
    INGEST_TASK_MEMORY("sleeper.ingest.task.memory", "4096"),
    INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS("sleeper.ingest.partition.refresh.period", "120"),
    INGEST_SOURCE_BUCKET("sleeper.ingest.source.bucket"),
    INGEST_RECORD_BATCH_TYPE("sleeper.ingest.record.batch.type", "arraylist"),
    INGEST_PARTITION_FILE_WRITER_TYPE("sleeper.ingest.partition.file.writer.type", "direct"),

    // ArrayList ingest
    MAX_RECORDS_TO_WRITE_LOCALLY("sleeper.ingest.max.local.records", "100000000"),
    MAX_IN_MEMORY_BATCH_SIZE("sleeper.ingest.memory.max.batch.size", "1000000"),

    // Arrow ingest
    ARROW_INGEST_WORKING_BUFFER_BYTES("sleeper.ingest.arrow.working.buffer.bytes", "268435456"),                    // 256M
    ARROW_INGEST_BATCH_BUFFER_BYTES("sleeper.ingest.arrow.batch.buffer.bytes", "1073741824"),                       // 1G
    ARROW_INGEST_MAX_LOCAL_STORE_BYTES("sleeper.ingest.arrow.max.local.store.bytes", "17179869184"),                // 16G
    ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS("sleeper.ingest.arrow.max.single.write.to.file.records", "1024"), // 1K

    // Status Store
    INGEST_STATUS_STORE_ENABLED("sleeper.ingest.status.store.enabled", "true"),
    INGEST_JOB_STATUS_TTL_IN_SECONDS("sleeper.ingest.job.status.ttl", "604800", Utils::isPositiveInteger), // Default is 1 week

    // Bulk Import - properties that are applicable to all bulk import platforms
    BULK_IMPORT_MIN_PARTITIONS_TO_USE_COALESCE("sleeper.bulk.import.min.partitions.coalesce", "100"),
    BULK_IMPORT_CLASS_NAME("sleeper.bulk.import.class.name", "sleeper.bulkimport.job.runner.dataframe.BulkImportJobDataframeRunner"),
    BULK_IMPORT_SPARK_SHUFFLE_MAPSTATUS_COMPRESSION_CODEC("sleeper.bulk.import.emr.spark.shuffle.mapStatus.compression.codec", "lz4"), // Stops "Decompression error: Version not supported" errors - only a value of "lz4" has been tested. This is used to set the value of spark.shuffle.mapStatus.compression.codec on the Spark configuration.
    BULK_IMPORT_SPARK_SPECULATION("sleeper.bulk.import.emr.spark.speculation", "false", Utils::isTrueOrFalse),
    // This is used to set the value of spark.speculation on the Spark configuration.
    BULK_IMPORT_SPARK_SPECULATION_QUANTILE("sleeper.bulk.import.spark.speculation.quantile", "0.75"), // This is used to set the value of spark.speculation.quantile on the Spark configuration.

    // Bulk import using EMR - these properties are used by both the persistent
    // and non-persistent EMR stacks
    BULK_IMPORT_EMR_EC2_KEYPAIR_NAME("sleeper.bulk.import.emr.keypair.name"),
    BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP("sleeper.bulk.import.emr.master.additional.security.group"),
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
    BULK_IMPORT_EMR_SPARK_EXECUTOR_MEMORY("sleeper.bulk.import.emr.spark.executor.memory", "16g"),
    BULK_IMPORT_EMR_SPARK_DRIVER_MEMORY("sleeper.bulk.import.emr.spark.driver.memory", BULK_IMPORT_EMR_SPARK_EXECUTOR_MEMORY.getDefaultValue()),
    BULK_IMPORT_EMR_SPARK_EXECUTOR_INSTANCES("sleeper.bulk.import.emr.spark.executor.instances", "29"),
    BULK_IMPORT_EMR_SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD("sleeper.bulk.import.emr.spark.yarn.executor.memory.overhead", "2g"),
    BULK_IMPORT_EMR_SPARK_YARN_DRIVER_MEMORY_OVERHEAD("sleeper.bulk.import.emr.spark.yarn.driver.memory.overhead", BULK_IMPORT_EMR_SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD.getDefaultValue()),
    BULK_IMPORT_EMR_SPARK_DEFAULT_PARALLELISM("sleeper.bulk.import.emr.spark.default.parallelism", "290"),
    BULK_IMPORT_EMR_SPARK_SQL_SHUFFLE_PARTITIONS("sleeper.bulk.import.emr.spark.sql.shuffle.partitions", BULK_IMPORT_EMR_SPARK_DEFAULT_PARALLELISM.getDefaultValue()),
    //  - Properties that are independent of the instance type and number of instances:
    BULK_IMPORT_EMR_SPARK_EXECUTOR_CORES("sleeper.bulk.import.emr.spark.executor.cores", "5"),
    BULK_IMPORT_EMR_SPARK_DRIVER_CORES("sleeper.bulk.import.emr.spark.driver.cores", BULK_IMPORT_EMR_SPARK_EXECUTOR_CORES.getDefaultValue()),
    BULK_IMPORT_EMR_SPARK_NETWORK_TIMEOUT("sleeper.bulk.import.emr.spark.network.timeout", "800s"),
    BULK_IMPORT_EMR_SPARK_EXECUTOR_HEARTBEAT_INTERVAL("sleeper.bulk.import.emr.spark.executor.heartbeat.interval", "60s"),
    BULK_IMPORT_EMR_SPARK_DYNAMIC_ALLOCATION_ENABLED("sleeper.bulk.import.emr.spark.dynamic.allocation.enabled", "false"),
    BULK_IMPORT_EMR_SPARK_MEMORY_FRACTION("sleeper.bulk.import.emr.spark.memory.fraction", "0.80"),
    BULK_IMPORT_EMR_SPARK_MEMORY_STORAGE_FRACTION("sleeper.bulk.import.emr.spark.memory.storage.fraction", "0.30"),
    BULK_IMPORT_EMR_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS("sleeper.bulk.import.emr.spark.executor.extra.java.options",
            "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'"),
    BULK_IMPORT_EMR_SPARK_DRIVER_EXTRA_JAVA_OPTIONS("sleeper.bulk.import.emr.spark.driver.extra.java.options",
            BULK_IMPORT_EMR_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS.getDefaultValue()),
    BULK_IMPORT_EMR_SPARK_YARN_SCHEDULER_REPORTER_THREAD_MAX_FAILURES("sleeper.bulk.import.emr.spark.yarn.scheduler.reporter.thread.max.failures",
            "5"),
    BULK_IMPORT_EMR_SPARK_STORAGE_LEVEL("sleeper.bulk.import.emr.spark.storage.level", "MEMORY_AND_DISK_SER"),
    BULK_IMPORT_EMR_SPARK_RDD_COMPRESS("sleeper.bulk.import.emr.spark.rdd.compress", "true"),
    BULK_IMPORT_EMR_SPARK_SHUFFLE_COMPRESS("sleeper.bulk.import.emr.spark.shuffle.compress", "true"),
    BULK_IMPORT_EMR_SPARK_SHUFFLE_SPILL_COMPRESS("sleeper.bulk.import.emr.spark.shuffle.spill.compress", "true"),
    BULK_IMPORT_EMR_EBS_VOLUME_SIZE_IN_GB("sleeper.bulk.import.emr.ebs.volume.size.gb", "256", Utils::isValidEbsSize),
    BULK_IMPORT_EMR_EBS_VOLUME_TYPE("sleeper.bulk.import.emr.ebs.volume.type", "gp2", Utils::isValidEbsVolumeType),
    BULK_IMPORT_EMR_EBS_VOLUMES_PER_INSTANCE("sleeper.bulk.import.emr.ebs.volumes.per.instance", "4", s -> Utils.isIntLtEqValue(s, 25)),

    // Bulk import using the non-persistent EMR approach
    DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL("sleeper.default.bulk.import.emr.release.label", "emr-6.8.0"),
    DEFAULT_BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE("sleeper.default.bulk.import.emr.master.instance.type", "m5.xlarge"),
    DEFAULT_BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE("sleeper.default.bulk.import.emr.executor.market.type", "SPOT", s -> ("SPOT".equals(s) || "ON_DEMAND".equals(s))),
    DEFAULT_BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE("sleeper.default.bulk.import.emr.executor.instance.type", "m5.4xlarge"),
    DEFAULT_BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS("sleeper.default.bulk.import.emr.executor.initial.instances", "2"),
    DEFAULT_BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS("sleeper.default.bulk.import.emr.executor.max.instances", "10"),

    // Bulk import using a persistent EMR cluster
    BULK_IMPORT_PERSISTENT_EMR_RELEASE_LABEL("sleeper.bulk.import.persistent.emr.release.label", DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL.defaultValue),
    BULK_IMPORT_PERSISTENT_EMR_MASTER_INSTANCE_TYPE("sleeper.bulk.import.persistent.emr.master.instance.type", DEFAULT_BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE.defaultValue),
    BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_INSTANCE_TYPE("sleeper.bulk.import.persistent.emr.core.instance.type", DEFAULT_BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE.defaultValue),
    BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING("sleeper.bulk.import.persistent.emr.use.managed.scaling", "true"),
    BULK_IMPORT_PERSISTENT_EMR_MIN_NUMBER_OF_INSTANCES("sleeper.bulk.import.persistent.emr.min.instances", "1"),
    BULK_IMPORT_PERSISTENT_EMR_MAX_NUMBER_OF_INSTANCES("sleeper.bulk.import.persistent.emr.max.instances", "10"),
    BULK_IMPORT_PERSISTENT_EMR_STEP_CONCURRENCY_LEVEL("sleeper.bulk.import.persistent.emr.step.concurrency.level", "2"),

    // Bulk import using EKS
    BULK_IMPORT_REPO("sleeper.bulk.import.eks.repo"),

    // Partition splitting
    PARTITION_SPLITTING_PERIOD_IN_MINUTES("sleeper.partition.splitting.period.minutes", "2"),
    MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB("sleeper.partition.splitting.files.maximum", "50"),
    FIND_PARTITIONS_TO_SPLIT_LAMBDA_MEMORY_IN_MB("sleeper.partition.splitting.finder.memory", "2048"),
    FIND_PARTITIONS_TO_SPLIT_TIMEOUT_IN_SECONDS("sleeper.partition.splitting.finder.timeout.seconds", "900"),
    SPLIT_PARTITIONS_LAMBDA_MEMORY_IN_MB("sleeper.partition.splitting.memory", "2048"),
    SPLIT_PARTITIONS_TIMEOUT_IN_SECONDS("sleeper.partition.splitting.timeout.seconds", "900"),
    DEFAULT_PARTITION_SPLIT_THRESHOLD("sleeper.default.partition.splitting.threshold", "1000000000"),

    // Garbage collection
    GARBAGE_COLLECTOR_PERIOD_IN_MINUTES("sleeper.gc.period.minutes", "15"),
    GARBAGE_COLLECTOR_LAMBDA_MEMORY_IN_MB("sleeper.gc.memory", "1024"),
    GARBAGE_COLLECTOR_BATCH_SIZE("sleeper.gc.batch.size", "2000"),
    DEFAULT_GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION("sleeper.default.gc.delay.seconds", "600"),

    // Compaction
    ECR_COMPACTION_REPO("sleeper.compaction.repo"),
    COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS("sleeper.compaction.queue.visibility.timeout.seconds", "900"),
    COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS("sleeper.compaction.keepalive.period.seconds", "300"),
    COMPACTION_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES("sleeper.compaction.job.creation.period.minutes", "1", Utils::isPositiveInteger),
    COMPACTION_JOB_CREATION_LAMBDA_MEMORY_IN_MB("sleeper.compaction.job.creation.memory", "1024"),
    COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS("sleeper.compaction.job.creation.timeout.seconds", "900", Utils::isValidLambdaTimeout),
    MAXIMUM_CONCURRENT_COMPACTION_TASKS("sleeper.compaction.max.concurrent.tasks", "300"),
    COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES("sleeper.compaction.task.creation.period.minutes", "1"), // >0
    COMPACTION_TASK_CPU_ARCHITECTURE("sleeper.compaction.task.cpu.architecture", "X86_64"),
    COMPACTION_TASK_ARM_CPU("sleeper.compaction.task.arm.cpu", "1024"),
    COMPACTION_TASK_ARM_MEMORY("sleeper.compaction.task.arm.memory", "4096"),
    COMPACTION_TASK_X86_CPU("sleeper.compaction.task.x86.cpu", "1024"),
    COMPACTION_TASK_X86_MEMORY("sleeper.compaction.task.x86.memory", "4096"),
    COMPACTION_STATUS_STORE_ENABLED("sleeper.compaction.status.store.enabled", "true"),
    COMPACTION_JOB_STATUS_TTL_IN_SECONDS("sleeper.compaction.job.status.ttl", "604800", Utils::isPositiveInteger), // Default is 1 week
    DEFAULT_COMPACTION_STRATEGY_CLASS("sleeper.default.compaction.strategy.class", "sleeper.compaction.strategy.impl.SizeRatioCompactionStrategy"),
    DEFAULT_COMPACTION_FILES_BATCH_SIZE("sleeper.default.compaction.files.batch.size", "11"),
    DEFAULT_SIZERATIO_COMPACTION_STRATEGY_RATIO("sleeper.default.table.compaction.strategy.sizeratio.ratio", "3"),
    DEFAULT_SIZERATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION("sleeper.default.table.compaction.strategy.sizeratio.max.concurrent.jobs.per.partition", "" + Integer.MAX_VALUE),

    // Query
    MAXIMUM_CONNECTIONS_TO_S3_FOR_QUERIES("sleeper.query.s3.max-connections", "1024", Utils::isPositiveInteger),
    QUERY_PROCESSOR_LAMBDA_MEMORY_IN_MB("sleeper.query.processor.memory", "2048"),
    QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS("sleeper.query.processor.timeout.seconds", "900"),
    QUERY_PROCESSING_LAMBDA_STATE_REFRESHING_PERIOD_IN_SECONDS("sleeper.query.processor.state.refresh.period.seconds", "60"),
    QUERY_PROCESSING_LAMBDA_RESULTS_BATCH_SIZE("sleeper.query.processor.results.batch.size", "2000"),
    QUERY_PROCESSOR_LAMBDA_RECORD_RETRIEVAL_THREADS("sleeper.query.processor.record.retrieval.threads", "10", Utils::isPositiveInteger),
    QUERY_TRACKER_ITEM_TTL_IN_DAYS("sleeper.query.tracker.ttl.days", "1", Utils::isPositiveInteger),
    QUERY_RESULTS_BUCKET_EXPIRY_IN_DAYS("sleeper.query.results.bucket.expiry.days", "7", Utils::isPositiveInteger),
    DEFAULT_RESULTS_ROW_GROUP_SIZE("sleeper.default.query.results.rowgroup.size", "" + (8 * 1024 * 1024)), // 8 MiB
    DEFAULT_RESULTS_PAGE_SIZE("sleeper.default.query.results.page.size", "" + (128 * 1024)), // 128 KiB

    // Dashboard
    DASHBOARD_TIME_WINDOW_MINUTES("sleeper.dashboard.time.window.minutes", "5", Utils::isPositiveInteger),

    // Logging levels
    LOGGING_LEVEL("sleeper.logging.level"),
    APACHE_LOGGING_LEVEL("sleeper.logging.apache.level"),
    PARQUET_LOGGING_LEVEL("sleeper.logging.parquet.level"),
    AWS_LOGGING_LEVEL("sleeper.logging.aws.level"),
    ROOT_LOGGING_LEVEL("sleeper.logging.root.level"),

    // Athena
    SPILL_BUCKET_AGE_OFF_IN_DAYS("sleeper.athena.spill.bucket.ageoff.days", "1"),
    ATHENA_COMPOSITE_HANDLER_CLASSES("sleeper.athena.handler.classes", "sleeper.athena.composite.SimpleCompositeHandler,sleeper.athena.composite.IteratorApplyingCompositeHandler"),
    ATHENA_COMPOSITE_HANDLER_MEMORY("sleeper.athena.handler.memory", "4096"),
    ATHENA_COMPOSITE_HANDLER_TIMEOUT_IN_SECONDS("sleeper.athena.handler.timeout.seconds", "900", Utils::isValidLambdaTimeout),

    // Default values
    DEFAULT_S3A_READAHEAD_RANGE("sleeper.default.fs.s3a.readahead.range", "64K"),
    DEFAULT_ROW_GROUP_SIZE("sleeper.default.rowgroup.size", "" + (8 * 1024 * 1024)), // 8 MiB
    DEFAULT_PAGE_SIZE("sleeper.default.page.size", "" + (128 * 1024)), // 128 KiB
    DEFAULT_COMPRESSION_CODEC("sleeper.default.compression.codec", "ZSTD", Utils::isValidCompressionCodec),
    DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY_ENABLED("sleeper.default.table.dynamo.pointintimerecovery", "false", Utils::isTrueOrFalse),
    DEFAULT_DYNAMO_STRONGLY_CONSISTENT_READS("sleeper.default.table.dynamo.strongly.consistent.reads", "false", Utils::isTrueOrFalse);

    private final String propertyName;
    private final String defaultValue;
    private final Predicate<String> validationPredicate;

    UserDefinedInstanceProperty(String propertyName) {
        this(propertyName, (String) null);
    }

    UserDefinedInstanceProperty(String propertyName, Predicate<String> validationPredicate) {
        this(propertyName, null, validationPredicate);
    }

    UserDefinedInstanceProperty(String propertyName, String defaultValue) {
        this(propertyName, defaultValue, (s) -> true);
    }

    UserDefinedInstanceProperty(String propertyName, String defaultValue, Predicate<String> validationPredicate) {
        this.propertyName = propertyName;
        this.defaultValue = defaultValue;
        this.validationPredicate = validationPredicate;
    }

    @Override
    public Predicate<String> validationPredicate() {
        return validationPredicate;
    }

    @Override
    public String getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String getPropertyName() {
        return propertyName;
    }
}
