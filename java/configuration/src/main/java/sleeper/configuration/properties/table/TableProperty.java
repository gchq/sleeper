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
package sleeper.configuration.properties.table;

import sleeper.configuration.Utils;
import sleeper.configuration.properties.SleeperProperty;

import java.util.Objects;
import java.util.function.Predicate;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_COMPACTION_STRATEGY_CLASS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_COMPRESSION_CODEC;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY_ENABLED;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_DYNAMO_STRONGLY_CONSISTENT_READS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_PAGE_SIZE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_PARTITION_SPLIT_THRESHOLD;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_ROW_GROUP_SIZE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_S3A_READAHEAD_RANGE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_SIZERATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_SIZERATIO_COMPACTION_STRATEGY_RATIO;

/**
 * These contain the table properties which are stored separately to the instance properties.
 */
public enum TableProperty implements ITableProperty {
    // User defined
    TABLE_NAME("sleeper.table.name", Objects::nonNull),
    SCHEMA("sleeper.table.schema", Objects::nonNull),
    ENCRYPTED("sleeper.table.encrypted", "true", null, s -> s.equals("true") || s.equals("false")),
    SCHEMA_FILE("sleeper.table.schema.file"),
    ROW_GROUP_SIZE("sleeper.table.rowgroup.size", DEFAULT_ROW_GROUP_SIZE),
    PAGE_SIZE("sleeper.table.page.size", DEFAULT_PAGE_SIZE),
    S3A_READAHEAD_RANGE("sleeper.table.fs.s3a.readahead.range", null, DEFAULT_S3A_READAHEAD_RANGE, Utils::isValidNumberOfBytes),
    COMPRESSION_CODEC("sleeper.table.compression.codec", null, DEFAULT_COMPRESSION_CODEC, Utils::isValidCompressionCodec),
    ITERATOR_CLASS_NAME("sleeper.table.iterator.class.name"),
    ITERATOR_CONFIG("sleeper.table.iterator.config"),
    SPLIT_POINTS_FILE("sleeper.table.splits.file"),
    SPLIT_POINTS_BASE64_ENCODED("sleeper.table.splits.base64.encoded", "false", null, s -> s.equals("true") || s.equals("false")),
    GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION("sleeper.table.gc.delay.seconds", DEFAULT_GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION),
    COMPACTION_STRATEGY_CLASS("sleeper.table.compaction.strategy.class", DEFAULT_COMPACTION_STRATEGY_CLASS),
    COMPACTION_FILES_BATCH_SIZE("sleeper.table.compaction.files.batch.size", DEFAULT_COMPACTION_FILES_BATCH_SIZE),
    PARTITION_SPLIT_THRESHOLD("sleeper.table.partition.splitting.threshold", DEFAULT_PARTITION_SPLIT_THRESHOLD),
    STATESTORE_CLASSNAME("sleeper.table.statestore.classname", "sleeper.statestore.dynamodb.DynamoDBStateStore"),

    BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE("sleeper.table.bulk.import.emr.master.instance.type", DEFAULT_BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE),
    BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE("sleeper.table.bulk.import.emr.executor.instance.type", DEFAULT_BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE),
    BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE("sleeper.table.bulk.import.emr.executor.market.type", DEFAULT_BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE),
    BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS("sleeper.table.bulk.import.emr.executor.initial.instances", DEFAULT_BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS),
    BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS("sleeper.table.bulk.import.emr.executor.max.instances", DEFAULT_BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS),
    BULK_IMPORT_EMR_RELEASE_LABEL("sleeper.table.bulk.import.emr.release.label", DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL),

    // Size ratio compaction strategy
    SIZE_RATIO_COMPACTION_STRATEGY_RATIO("sleeper.table.compaction.strategy.sizeratio.ratio", DEFAULT_SIZERATIO_COMPACTION_STRATEGY_RATIO),
    SIZE_RATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION("sleeper.table.compaction.strategy.sizeratio.max.concurrent.jobs.per.partition", DEFAULT_SIZERATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION),

    // System defined
    SPLIT_POINTS_KEY("sleeper.table.splits.key"),
    DATA_BUCKET("sleeper.table.data.bucket"),
    // DynamoDBStateStore properties
    ACTIVE_FILEINFO_TABLENAME("sleeper.table.metadata.dynamo.active.table"),
    READY_FOR_GC_FILEINFO_TABLENAME("sleeper.table.metadata.dynamo.gc.table"),
    PARTITION_TABLENAME("sleeper.table.metadata.dynamo.partition.table"),
    DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY("sleeper.table.metadata.dynamo.pointintimerecovery", DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY_ENABLED),
    DYNAMODB_STRONGLY_CONSISTENT_READS("sleeper.table.metadata.dynamo.consistent.reads", null, DEFAULT_DYNAMO_STRONGLY_CONSISTENT_READS,
            s -> s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false")),
    // S3StateStore properties
    REVISION_TABLENAME("sleeper.table.metadata.s3.dynamo.revision.table"),
    S3_STATE_STORE_DYNAMO_POINT_IN_TIME_RECOVERY("sleeper.table.metadata.s3.dynamo.pointintimerecovery", DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY_ENABLED);

    private final String propertyName;
    private final String defaultValue;
    private final Predicate<String> validationPredicate;
    private final SleeperProperty defaultProperty;

    TableProperty(String propertyName) {
        this(propertyName, (String) null);
    }

    TableProperty(String propertyName, Predicate<String> validationPredicate) {
        this(propertyName, null, null, validationPredicate);
    }

    TableProperty(String propertyName, String defaultValue) {
        this(propertyName, defaultValue, null, (s) -> true);
    }

    TableProperty(String propertyName, SleeperProperty defaultProperty) {
        this(propertyName, null, defaultProperty, (s) -> true);
    }

    TableProperty(String propertyName, String defaultValue, SleeperProperty defaultProperty, Predicate<String> validationPredicate) {
        this.propertyName = propertyName;
        this.defaultValue = defaultValue;
        this.defaultProperty = defaultProperty;
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

    @Override
    public SleeperProperty getDefaultProperty() {
        return defaultProperty;
    }
}

