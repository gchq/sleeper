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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
// Suppress as this class will always be referenced before impl class, so initialization behaviour will be deterministic
@SuppressFBWarnings("IC_SUPERCLASS_USES_SUBCLASS_DURING_INITIALIZATION")
public interface TableProperty extends SleeperProperty {
    // User defined
    TableProperty TABLE_NAME = named("sleeper.table.name")
            .validationPredicate(Objects::nonNull)
            .description("A unique name identifying this table.")
            .build();
    TableProperty SCHEMA = named("sleeper.table.schema")
            .validationPredicate(Objects::nonNull)
            .description("The schema representing the structure of this table.")
            .build();
    TableProperty ENCRYPTED = named("sleeper.table.encrypted")
            .defaultValue("true")
            .validationPredicate(s -> s.equals("true") || s.equals("false"))
            .description("Whether or not to encrypt the table. If set to \"true\", all data at rest will be encrypted.\n" +
                    "When this is changed, existing files will retain their encryption status. Further compactions may " +
                    "apply the new encryption status for that data.\n" +
                    "See also: https://docs.aws.amazon.com/AmazonS3/latest/userguide/default-bucket-encryption.html")
            .runCDKDeployWhenChanged(true).build();
    TableProperty ROW_GROUP_SIZE = named("sleeper.table.rowgroup.size")
            .defaultProperty(DEFAULT_ROW_GROUP_SIZE)
            .description("The size of the row group in the Parquet files - defaults to the value in the instance properties.")
            .build();
    TableProperty PAGE_SIZE = named("sleeper.table.page.size")
            .defaultProperty(DEFAULT_PAGE_SIZE)
            .description("The size of the page in the Parquet files - defaults to the value in the instance properties.")
            .build();
    TableProperty S3A_READAHEAD_RANGE = named("sleeper.table.fs.s3a.readahead.range")
            .defaultProperty(DEFAULT_S3A_READAHEAD_RANGE)
            .validationPredicate(Utils::isValidNumberOfBytes)
            .description("The S3 readahead range - defaults to the value in the instance properties.")
            .build();
    TableProperty COMPRESSION_CODEC = named("sleeper.table.compression.codec")
            .defaultProperty(DEFAULT_COMPRESSION_CODEC)
            .validationPredicate(Utils::isValidCompressionCodec)
            .description("The compression codec to use for this table. Defaults to the value in the instance properties.")
            .build();
    TableProperty ITERATOR_CLASS_NAME = named("sleeper.table.iterator.class.name")
            .description("Fully qualified class of a custom iterator to use when iterating over the values in this table.  " +
                    "Defaults to nothing.")
            .build();
    TableProperty ITERATOR_CONFIG = named("sleeper.table.iterator.config")
            .description("Iterator configuration. An iterator will be initialised with the following configuration.")
            .build();
    TableProperty SPLIT_POINTS_FILE = named("sleeper.table.splits.file")
            .description("Splits file which will be used to initialise the partitions for this table. Defaults to nothing and the  " +
                    "table will be created with a single root partition.")
            .runCDKDeployWhenChanged(true).build();
    TableProperty SPLIT_POINTS_BASE64_ENCODED = named("sleeper.table.splits.base64.encoded")
            .defaultValue("false")
            .validationPredicate(s -> s.equals("true") || s.equals("false"))
            .description("Flag to set if you have base64 encoded the split points (only used for string key types and defaults to false).")
            .build();
    TableProperty GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION = named("sleeper.table.gc.delay.seconds")
            .defaultProperty(DEFAULT_GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION)
            .description("A file will not be deleted until this number of seconds have passed after it has been marked as ready for " +
                    "garbage collection. The reason for not deleting files immediately after they have been marked as ready for " +
                    "garbage collection is that they may still be in use by queries. Defaults to the value set in the instance " +
                    "properties.")
            .build();
    TableProperty COMPACTION_STRATEGY_CLASS = named("sleeper.table.compaction.strategy.class")
            .defaultProperty(DEFAULT_COMPACTION_STRATEGY_CLASS)
            .description("The name of the class that defines how compaction jobs should be created.\n" +
                    "This should implement sleeper.compaction.strategy.CompactionStrategy. Defaults to the strategy used by the whole " +
                    "instance (set in the instance properties).")
            .build();
    TableProperty COMPACTION_FILES_BATCH_SIZE = named("sleeper.table.compaction.files.batch.size")
            .defaultProperty(DEFAULT_COMPACTION_FILES_BATCH_SIZE)
            .description("The minimum number of files to read in a compaction job. Note that the state store " +
                    "must support atomic updates for this many files. For the DynamoDBStateStore this is 11.\n" +
                    "(NB This does not apply to splitting jobs which will run even if there is only 1 file.)")
            .build();
    TableProperty PARTITION_SPLIT_THRESHOLD = named("sleeper.table.partition.splitting.threshold")
            .defaultProperty(DEFAULT_PARTITION_SPLIT_THRESHOLD)
            .description("Partitions in this table with more than the following number of records in will be split.")
            .build();
    TableProperty STATESTORE_CLASSNAME = named("sleeper.table.statestore.classname")
            .defaultValue("sleeper.statestore.dynamodb.DynamoDBStateStore")
            .description("The name of the class used for the metadata store. The default is DynamoDBStateStore. " +
                    "An alternative option is the S3StateStore.")
            .build();

    TableProperty BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE = named("sleeper.table.bulk.import.emr.master.instance.type")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE)
            .description("(EMR mode only) The EC2 instance type to be used for the master node of the EMR cluster. This value " +
                    "overrides the default value in the instance properties. It can be overridden by a value in the bulk " +
                    "import job specification.")
            .build();
    TableProperty BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE = named("sleeper.table.bulk.import.emr.executor.instance.type")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE)
            .description("(EMR mode only) The EC2 instance type to be used for the executor nodes of the EMR cluster. This value " +
                    "overrides the default value in the instance properties. It can be overridden by a value in the bulk " +
                    "import job specification.")
            .build();
    TableProperty BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE = named("sleeper.table.bulk.import.emr.executor.market.type")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE)
            .build();
    TableProperty BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS = named("sleeper.table.bulk.import.emr.executor.initial.instances")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS)
            .description("(EMR mode only) The initial number of EC2 instances to be used as executors in the EMR cluster. This value " +
                    "overrides the default value in the instance properties. It can be overridden by a value in the bulk " +
                    "import job specification.")
            .build();
    TableProperty BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS = named("sleeper.table.bulk.import.emr.executor.max.instances")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS)
            .description("(EMR mode only) The maximum number of EC2 instances to be used as executors in the EMR cluster. This value " +
                    "overrides the default value in the instance properties. It can be overridden by a value in the bulk " +
                    "import job specification.")
            .build();
    TableProperty BULK_IMPORT_EMR_RELEASE_LABEL = named("sleeper.table.bulk.import.emr.release.label")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL)
            .description("(EMR mode only) The EMR release label to be used when creating an EMR cluster for bulk importing data " +
                    "using Spark running on EMR. This value overrides the default value in the instance properties. It can " +
                    "be overridden by a value in the bulk import job specification.")
            .build();

    // Size ratio compaction strategy
    TableProperty SIZE_RATIO_COMPACTION_STRATEGY_RATIO = named("sleeper.table.compaction.strategy.sizeratio.ratio")
            .defaultProperty(DEFAULT_SIZERATIO_COMPACTION_STRATEGY_RATIO)
            .description("Used by the SizeRatioCompactionStrategy to decide if a group of files should be compacted.\n" +
                    "If the file sizes are s_1, ..., s_n then the files are compacted if s_1 + ... + s_{n-1} >= ratio * s_n.")
            .build();
    TableProperty SIZE_RATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION = named("sleeper.table.compaction.strategy.sizeratio.max.concurrent.jobs.per.partition")
            .defaultProperty(DEFAULT_SIZERATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION)
            .description("Used by the SizeRatioCompactionStrategy to control the maximum number of jobs that can be running " +
                    "concurrently per partition.")
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

    static List<TableProperty> getAll() {
        return TablePropertyImpl.getAll();
    }

    static Optional<TableProperty> getByName(String propertyName) {
        return TablePropertyImpl.getByName(propertyName);
    }

    static boolean has(String propertyName) {
        return TablePropertyImpl.getByName(propertyName).isPresent();
    }

    SleeperProperty getDefaultProperty();
}
