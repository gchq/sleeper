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
package sleeper.configuration.properties.table;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import sleeper.configuration.Utils;
import sleeper.configuration.properties.SleeperProperty;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
import static sleeper.configuration.properties.table.TablePropertyImpl.named;

/**
 * These contain the table properties which are stored separately to the instance properties.
 */
@SuppressFBWarnings("IC_SUPERCLASS_USES_SUBCLASS_DURING_INITIALIZATION") // Suppress as superclass is an interface
public interface TableProperty extends SleeperProperty {
    // User defined
    TableProperty TABLE_NAME = named("sleeper.table.name")
            .validationPredicate(Objects::nonNull)
            .build();
    TableProperty SCHEMA = named("sleeper.table.schema")
            .validationPredicate(Objects::nonNull)
            .build();
    TableProperty ENCRYPTED = named("sleeper.table.encrypted")
            .defaultValue("true")
            .validationPredicate(s -> s.equals("true") || s.equals("false"))
            .build();
    TableProperty SCHEMA_FILE = named("sleeper.table.schema.file").build();
    TableProperty ROW_GROUP_SIZE = named("sleeper.table.rowgroup.size")
            .defaultProperty(DEFAULT_ROW_GROUP_SIZE)
            .build();
    TableProperty PAGE_SIZE = named("sleeper.table.page.size")
            .defaultProperty(DEFAULT_PAGE_SIZE)
            .build();
    TableProperty S3A_READAHEAD_RANGE = named("sleeper.table.fs.s3a.readahead.range")
            .defaultProperty(DEFAULT_S3A_READAHEAD_RANGE)
            .validationPredicate(Utils::isValidNumberOfBytes)
            .build();
    TableProperty COMPRESSION_CODEC = named("sleeper.table.compression.codec")
            .defaultProperty(DEFAULT_COMPRESSION_CODEC)
            .validationPredicate(Utils::isValidCompressionCodec)
            .build();
    TableProperty ITERATOR_CLASS_NAME = named("sleeper.table.iterator.class.name").build();
    TableProperty ITERATOR_CONFIG = named("sleeper.table.iterator.config").build();
    TableProperty SPLIT_POINTS_FILE = named("sleeper.table.splits.file").build();
    TableProperty SPLIT_POINTS_BASE64_ENCODED = named("sleeper.table.splits.base64.encoded")
            .defaultValue("false")
            .validationPredicate(s -> s.equals("true") || s.equals("false"))
            .build();
    TableProperty GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION = named("sleeper.table.gc.delay.seconds")
            .defaultProperty(DEFAULT_GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION)
            .build();
    TableProperty COMPACTION_STRATEGY_CLASS = named("sleeper.table.compaction.strategy.class")
            .defaultProperty(DEFAULT_COMPACTION_STRATEGY_CLASS)
            .build();
    TableProperty COMPACTION_FILES_BATCH_SIZE = named("sleeper.table.compaction.files.batch.size")
            .defaultProperty(DEFAULT_COMPACTION_FILES_BATCH_SIZE)
            .build();
    TableProperty PARTITION_SPLIT_THRESHOLD = named("sleeper.table.partition.splitting.threshold")
            .defaultProperty(DEFAULT_PARTITION_SPLIT_THRESHOLD)
            .build();
    TableProperty STATESTORE_CLASSNAME = named("sleeper.table.statestore.classname")
            .defaultValue("sleeper.statestore.dynamodb.DynamoDBStateStore")
            .build();

    TableProperty BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE = named("sleeper.table.bulk.import.emr.master.instance.type")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE)
            .build();
    TableProperty BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE = named("sleeper.table.bulk.import.emr.executor.instance.type")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE)
            .build();
    TableProperty BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE = named("sleeper.table.bulk.import.emr.executor.market.type")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE)
            .build();
    TableProperty BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS = named("sleeper.table.bulk.import.emr.executor.initial.instances")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS)
            .build();
    TableProperty BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS = named("sleeper.table.bulk.import.emr.executor.max.instances")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS)
            .build();
    TableProperty BULK_IMPORT_EMR_RELEASE_LABEL = named("sleeper.table.bulk.import.emr.release.label")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL)
            .build();

    // Size ratio compaction strategy
    TableProperty SIZE_RATIO_COMPACTION_STRATEGY_RATIO = named("sleeper.table.compaction.strategy.sizeratio.ratio")
            .defaultProperty(DEFAULT_SIZERATIO_COMPACTION_STRATEGY_RATIO)
            .build();
    TableProperty SIZE_RATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION = named("sleeper.table.compaction.strategy.sizeratio.max.concurrent.jobs.per.partition")
            .defaultProperty(DEFAULT_SIZERATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION)
            .build();

    // System defined
    TableProperty SPLIT_POINTS_KEY = named("sleeper.table.splits.key").build();
    TableProperty DATA_BUCKET = named("sleeper.table.data.bucket").build();
    // DynamoDBStateStore properties
    TableProperty ACTIVE_FILEINFO_TABLENAME = named("sleeper.table.metadata.dynamo.active.table").build();
    TableProperty READY_FOR_GC_FILEINFO_TABLENAME = named("sleeper.table.metadata.dynamo.gc.table").build();
    TableProperty PARTITION_TABLENAME = named("sleeper.table.metadata.dynamo.partition.table").build();
    TableProperty DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY = named("sleeper.table.metadata.dynamo.pointintimerecovery")
            .defaultProperty(DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY_ENABLED)
            .build();
    TableProperty DYNAMODB_STRONGLY_CONSISTENT_READS = named("sleeper.table.metadata.dynamo.consistent.reads")
            .defaultProperty(DEFAULT_DYNAMO_STRONGLY_CONSISTENT_READS)
            .validationPredicate(s -> s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false"))
            .build();
    // S3StateStore properties
    TableProperty REVISION_TABLENAME = named("sleeper.table.metadata.s3.dynamo.revision.table").build();
    TableProperty S3_STATE_STORE_DYNAMO_POINT_IN_TIME_RECOVERY = named("sleeper.table.metadata.s3.dynamo.pointintimerecovery")
            .defaultProperty(DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY_ENABLED)
            .build();

    static TableProperty[] values() {
        return getAll().toArray(new TableProperty[0]);
    }

    private static List<TableProperty> getAll() {
        Field[] fields = TableProperty.class.getDeclaredFields();
        List<TableProperty> properties = new ArrayList<>(fields.length);
        for (Field field : fields) {
            try {
                properties.add((TableProperty) field.get(null));
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(
                        "Could not instantiate list of all user defined instance properties, failed reading " + field.getName(), e);
            }
        }
        return Collections.unmodifiableList(properties);
    }

    SleeperProperty getDefaultProperty();
}

