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
    ARROW_INGEST_WORKING_BUFFER_BYTES("sleeper.ingest.arrow.working.buffer.bytes", "134217728"),                    // 128M
    ARROW_INGEST_BATCH_BUFFER_BYTES("sleeper.ingest.arrow.batch.buffer.bytes", "1073741824"),                       // 1G
    ARROW_INGEST_MAX_LOCAL_STORE_BYTES("sleeper.ingest.arrow.max.local.store.bytes", "17179869184"),                // 16G
    ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS("sleeper.ingest.arrow.max.single.write.to.file.records", "1024"), // 1K

    // Bulk Import
    BULK_IMPORT_MIN_PARTITIONS_TO_USE_COALESCE("sleeper.bulk.import.min.partitions.coalesce", "100"),
    DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL("sleeper.default.bulk.import.emr.release.label", "emr-6.4.0"),
    DEFAULT_BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE("sleeper.default.bulk.import.emr.master.instance.type", "m5.xlarge"),
    DEFAULT_BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE("sleeper.default.bulk.import.emr.executor.market.type", "SPOT", s -> ("SPOT".equals(s) || "ON_DEMAND".equals(s))),
    DEFAULT_BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE("sleeper.default.bulk.import.emr.executor.instance.type", "m5.4xlarge"),
    DEFAULT_BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS("sleeper.default.bulk.import.emr.executor.initial.instances", "2"),
    DEFAULT_BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS("sleeper.default.bulk.import.emr.executor.max.instances", "10"),

    // Bulk import using EMR - these properties are used by both the persistent
    // and non-persistent EMR stacks
    BULK_IMPORT_EMR_BUCKET("sleeper.bulk.import.emr.bucket"),
    BULK_IMPORT_EMR_BUCKET_CREATE("sleeper.bulk.import.emr.bucket.create", "true"),
    BULK_IMPORT_EC2_KEY_NAME("sleeper.bulk.import.emr.keypair.name"),
    BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP("sleeper.bulk.import.emr.master.additional.security.group"),

    // Bulk import using persistent EMR cluster
    BULK_IMPORT_PERSISTENT_EMR_RELEASE_LABEL("sleeper.bulk.import.persistent.emr.release.label", "emr-6.4.0"),
    BULK_IMPORT_PERSISTENT_EMR_MASTER_INSTANCE_TYPE("sleeper.bulk.import.persistent.emr.master.instance.type", DEFAULT_BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE.defaultValue),
    BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_INSTANCE_TYPE("sleeper.bulk.import.persistent.emr.core.instance.type", DEFAULT_BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE.defaultValue),
    BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING("sleeper.bulk.import.persistent.emr.use.managed.scaling", "true"),
    BULK_IMPORT_PERSISTENT_EMR_MIN_NUMBER_OF_EXECUTORS("sleeper.bulk.import.persistent.emr.min.instances", "1"),
    BULK_IMPORT_PERSISTENT_EMR_MAX_NUMBER_OF_EXECUTORS("sleeper.bulk.import.persistent.emr.max.instances", "10"),

    // Bulk import using EKS
    BULK_IMPORT_REPO("sleeper.bulk.import.eks.repo"),

    // Bulk import common properties
    DEFAULT_BULK_IMPORT_SPARK_SHUFFLE_MAPSTATUS_COMPRESSION_CODEC("sleeper.default.bulk.import.spark.shuffle.mapStatus.compression.codec", "lz4"), // Stops "Decompression error: Version not supported" errors - only a value of "lz4" has been tested. This is used to set the value of spark.shuffle.mapStatus.compression.codec on the Spark configuration.
    DEFAULT_BULK_IMPORT_SPARK_SPECULATION("sleeper.default.bulk.import.spark.speculation", "false", Utils::isTrueOrFalse),
    // This is used to set the value of spark.speculation on the Spark configuration.
    DEFAULT_BULK_IMPORT_SPARK_SPECULATION_QUANTILE("sleeper.default.bulk.import.spark.speculation.quantile", "0.75"), // This is used to set the value of spark.speculation.quantile on the Spark configuration.

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
    COMPACTION_TASK_CPU("sleeper.compaction.task.cpu", "2048"),
    COMPACTION_TASK_MEMORY("sleeper.compaction.task.memory", "4096"),
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
