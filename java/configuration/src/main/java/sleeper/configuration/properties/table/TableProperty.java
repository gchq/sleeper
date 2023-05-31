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
import sleeper.configuration.properties.PropertyGroup;
import sleeper.configuration.properties.SleeperProperty;
import sleeper.configuration.properties.SleeperPropertyIndex;
import sleeper.configuration.properties.validation.BatchIngestMode;

import java.util.List;
import java.util.Objects;

import static sleeper.configuration.Utils.describeEnumValuesInLowerCase;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_COMPACTION_STRATEGY_CLASS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_COMPRESSION_CODEC;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_DICTIONARY_ENCODING_FOR_VALUE_FIELDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY_ENABLED;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_DYNAMO_STRONGLY_CONSISTENT_READS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_INGEST_BATCHER_INGEST_MODE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_INGEST_BATCHER_MAX_FILE_AGE_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_INGEST_BATCHER_MAX_JOB_FILES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_INGEST_BATCHER_MAX_JOB_SIZE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_INGEST_BATCHER_MIN_JOB_FILES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_INGEST_BATCHER_MIN_JOB_SIZE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_INGEST_BATCHER_TRACKING_TTL_MINUTES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_PAGE_SIZE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_PARTITION_SPLIT_THRESHOLD;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_ROW_GROUP_SIZE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_S3A_READAHEAD_RANGE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_SIZERATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_SIZERATIO_COMPACTION_STRATEGY_RATIO;

/**
 * These contain the table properties which are stored separately to the instance properties.
 */
// Suppress as this class will always be referenced before impl class, so initialization behaviour will be deterministic
@SuppressFBWarnings("IC_SUPERCLASS_USES_SUBCLASS_DURING_INITIALIZATION")
public interface TableProperty extends SleeperProperty {
    // User defined
    TableProperty TABLE_NAME = Index.propertyBuilder("sleeper.table.name")
            .validationPredicate(Objects::nonNull)
            .description("A unique name identifying this table.")
            .propertyGroup(TablePropertyGroup.DATA_DEFINITION)
            .editable(false).build();
    TableProperty SCHEMA = Index.propertyBuilder("sleeper.table.schema")
            .validationPredicate(Objects::nonNull)
            .description("The schema representing the structure of this table.")
            .propertyGroup(TablePropertyGroup.DATA_DEFINITION)
            .editable(false)
            .includedInTemplate(false).build();
    TableProperty ITERATOR_CLASS_NAME = Index.propertyBuilder("sleeper.table.iterator.class.name")
            .description("Fully qualified class of a custom iterator to use when iterating over the values in this table. " +
                    "Defaults to nothing.")
            .propertyGroup(TablePropertyGroup.DATA_DEFINITION)
            .build();
    TableProperty ITERATOR_CONFIG = Index.propertyBuilder("sleeper.table.iterator.config")
            .description("Iterator configuration. An iterator will be initialised with the following configuration.")
            .propertyGroup(TablePropertyGroup.DATA_DEFINITION)
            .build();
    TableProperty ENCRYPTED = Index.propertyBuilder("sleeper.table.encrypted")
            .defaultValue("true")
            .validationPredicate(s -> s.equals("true") || s.equals("false"))
            .description("Whether or not to encrypt the table. If set to \"true\", all data at rest will be encrypted.\n" +
                    "When this is changed, existing files will retain their encryption status. Further compactions may " +
                    "apply the new encryption status for that data.\n" +
                    "See also: https://docs.aws.amazon.com/AmazonS3/latest/userguide/default-bucket-encryption.html")
            .propertyGroup(TablePropertyGroup.DATA_STORAGE)
            .runCDKDeployWhenChanged(true)
            .build();
    TableProperty ROW_GROUP_SIZE = Index.propertyBuilder("sleeper.table.rowgroup.size")
            .defaultProperty(DEFAULT_ROW_GROUP_SIZE)
            .description("The size of the row group in the Parquet files - defaults to the value in the instance properties.")
            .propertyGroup(TablePropertyGroup.DATA_STORAGE)
            .build();
    TableProperty PAGE_SIZE = Index.propertyBuilder("sleeper.table.page.size")
            .defaultProperty(DEFAULT_PAGE_SIZE)
            .description("The size of the page in the Parquet files - defaults to the value in the instance properties.")
            .propertyGroup(TablePropertyGroup.DATA_STORAGE)
            .build();
    TableProperty DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS = Index.propertyBuilder("sleeper.table.parquet.dictionary.encoding.rowkey.fields")
            .defaultProperty(DEFAULT_DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS)
            .description("Whether dictionary encoding should be used for row key columns in the Parquet files.")
            .propertyGroup(TablePropertyGroup.DATA_STORAGE)
            .build();
    TableProperty DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS = Index.propertyBuilder("sleeper.table.parquet.dictionary.encoding.sortkey.fields")
            .defaultProperty(DEFAULT_DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS)
            .description("Whether dictionary encoding should be used for sort key columns in the Parquet files.")
            .propertyGroup(TablePropertyGroup.DATA_STORAGE)
            .build();
    TableProperty DICTIONARY_ENCODING_FOR_VALUE_FIELDS = Index.propertyBuilder("sleeper.table.parquet.dictionary.encoding.value.fields")
            .defaultProperty(DEFAULT_DICTIONARY_ENCODING_FOR_VALUE_FIELDS)
            .description("Whether dictionary encoding should be used for value columns in the Parquet files.")
            .propertyGroup(TablePropertyGroup.DATA_STORAGE)
            .build();
    TableProperty S3A_READAHEAD_RANGE = Index.propertyBuilder("sleeper.table.fs.s3a.readahead.range")
            .defaultProperty(DEFAULT_S3A_READAHEAD_RANGE)
            .description("The S3 readahead range - defaults to the value in the instance properties.")
            .propertyGroup(TablePropertyGroup.DATA_STORAGE)
            .build();
    TableProperty COMPRESSION_CODEC = Index.propertyBuilder("sleeper.table.compression.codec")
            .defaultProperty(DEFAULT_COMPRESSION_CODEC)
            .description("The compression codec to use for this table. Defaults to the value in the instance properties.\n" +
                    "Valid values are: " + describeEnumValuesInLowerCase(CompressionCodec.class))
            .propertyGroup(TablePropertyGroup.DATA_STORAGE)
            .build();
    TableProperty SPLIT_POINTS_FILE = Index.propertyBuilder("sleeper.table.splits.file")
            .description("Splits file which will be used to initialise the partitions for this table. Defaults to nothing and the " +
                    "table will be created with a single root partition.")
            .propertyGroup(TablePropertyGroup.PARTITION_SPLITTING)
            .runCDKDeployWhenChanged(true).build();
    TableProperty SPLIT_POINTS_BASE64_ENCODED = Index.propertyBuilder("sleeper.table.splits.base64.encoded")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse)
            .description("Flag to set if you have base64 encoded the split points (only used for string key types and defaults to false).")
            .propertyGroup(TablePropertyGroup.PARTITION_SPLITTING)
            .runCDKDeployWhenChanged(true).build();
    TableProperty GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION = Index.propertyBuilder("sleeper.table.gc.delay.seconds")
            .defaultProperty(DEFAULT_GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION)
            .description("A file will not be deleted until this number of seconds have passed after it has been marked as ready for " +
                    "garbage collection. The reason for not deleting files immediately after they have been marked as ready for " +
                    "garbage collection is that they may still be in use by queries. Defaults to the value set in the instance " +
                    "properties.")
            .propertyGroup(TablePropertyGroup.DATA_STORAGE)
            .build();
    TableProperty COMPACTION_STRATEGY_CLASS = Index.propertyBuilder("sleeper.table.compaction.strategy.class")
            .defaultProperty(DEFAULT_COMPACTION_STRATEGY_CLASS)
            .description("The name of the class that defines how compaction jobs should be created.\n" +
                    "This should implement sleeper.compaction.strategy.CompactionStrategy. Defaults to the strategy used by the whole " +
                    "instance (set in the instance properties).")
            .propertyGroup(TablePropertyGroup.COMPACTION)
            .build();
    TableProperty COMPACTION_FILES_BATCH_SIZE = Index.propertyBuilder("sleeper.table.compaction.files.batch.size")
            .defaultProperty(DEFAULT_COMPACTION_FILES_BATCH_SIZE)
            .description("The number of files to read in a compaction job. Note that the state store " +
                    "must support atomic updates for this many files.\n" +
                    "The DynamoDBStateStore must be able to atomically apply 2 updates to create the output files for a " +
                    "splitting compaction, and 2 updates for each input file to mark them as ready for garbage " +
                    "collection. There's a limit of 100 atomic updates, which equates to 48 files in a compaction.\n" +
                    "See also: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html\n" +
                    "(NB This does not apply to splitting jobs which will run even if there is only 1 file.)")
            .propertyGroup(TablePropertyGroup.COMPACTION)
            .build();
    TableProperty PARTITION_SPLIT_THRESHOLD = Index.propertyBuilder("sleeper.table.partition.splitting.threshold")
            .defaultProperty(DEFAULT_PARTITION_SPLIT_THRESHOLD)
            .description("Partitions in this table with more than the following number of records in will be split.")
            .propertyGroup(TablePropertyGroup.PARTITION_SPLITTING)
            .build();
    TableProperty STATESTORE_CLASSNAME = Index.propertyBuilder("sleeper.table.statestore.classname")
            .defaultValue("sleeper.statestore.dynamodb.DynamoDBStateStore")
            .description("The name of the class used for the metadata store. The default is DynamoDBStateStore. " +
                    "An alternative option is the S3StateStore.")
            .propertyGroup(TablePropertyGroup.METADATA)
            .editable(false).build();
    TableProperty DYNAMODB_STRONGLY_CONSISTENT_READS = Index.propertyBuilder("sleeper.table.metadata.dynamo.consistent.reads")
            .defaultProperty(DEFAULT_DYNAMO_STRONGLY_CONSISTENT_READS)
            .description("This specifies whether queries and scans against DynamoDB tables used in the DynamoDB state store " +
                    "are strongly consistent.")
            .propertyGroup(TablePropertyGroup.METADATA)
            .build();
    TableProperty S3_STATE_STORE_DYNAMO_POINT_IN_TIME_RECOVERY = Index.propertyBuilder("sleeper.table.metadata.s3.dynamo.pointintimerecovery")
            .defaultProperty(DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY_ENABLED)
            .description("This specifies whether point in time recovery is enabled for the revision table if " +
                    "the S3StateStore is used.")
            .propertyGroup(TablePropertyGroup.METADATA)
            .runCDKDeployWhenChanged(true).build();
    TableProperty BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE = Index.propertyBuilder("sleeper.table.bulk.import.emr.master.instance.type")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE)
            .description("(EMR mode only) The EC2 instance type to be used for the master node of the EMR cluster. This value " +
                    "overrides the default value in the instance properties. It can be overridden by a value in the bulk " +
                    "import job specification.")
            .propertyGroup(TablePropertyGroup.BULK_IMPORT)
            .build();
    TableProperty BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE = Index.propertyBuilder("sleeper.table.bulk.import.emr.executor.instance.type")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE)
            .description("(EMR mode only) The EC2 instance type to be used for the executor nodes of the EMR cluster. This value " +
                    "overrides the default value in the instance properties. It can be overridden by a value in the bulk " +
                    "import job specification.")
            .propertyGroup(TablePropertyGroup.BULK_IMPORT)
            .build();
    TableProperty BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE = Index.propertyBuilder("sleeper.table.bulk.import.emr.executor.market.type")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE)
            .description("(EMR mode only) The purchasing option to be used for the executor nodes of the EMR cluster.\n" +
                    "Valid values are ON_DEMAND or SPOT.")
            .propertyGroup(TablePropertyGroup.BULK_IMPORT)
            .build();
    TableProperty BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS = Index.propertyBuilder("sleeper.table.bulk.import.emr.executor.initial.instances")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS)
            .description("(EMR mode only) The initial number of EC2 instances to be used as executors in the EMR cluster. This value " +
                    "overrides the default value in the instance properties. It can be overridden by a value in the bulk " +
                    "import job specification.")
            .propertyGroup(TablePropertyGroup.BULK_IMPORT)
            .build();
    TableProperty BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS = Index.propertyBuilder("sleeper.table.bulk.import.emr.executor.max.instances")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS)
            .description("(EMR mode only) The maximum number of EC2 instances to be used as executors in the EMR cluster. This value " +
                    "overrides the default value in the instance properties. It can be overridden by a value in the bulk " +
                    "import job specification.")
            .propertyGroup(TablePropertyGroup.BULK_IMPORT)
            .build();
    TableProperty BULK_IMPORT_EMR_RELEASE_LABEL = Index.propertyBuilder("sleeper.table.bulk.import.emr.release.label")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL)
            .description("(EMR mode only) The EMR release label to be used when creating an EMR cluster for bulk importing data " +
                    "using Spark running on EMR. This value overrides the default value in the instance properties. It can " +
                    "be overridden by a value in the bulk import job specification.")
            .propertyGroup(TablePropertyGroup.BULK_IMPORT)
            .build();
    TableProperty BULK_IMPORT_MIN_LEAF_PARTITION_COUNT = Index.propertyBuilder("sleeper.table.bulk.import.min.leaf.partitions")
            .description("Specifies the minimum number of leaf partitions that are needed to run a bulk import job. " +
                    "If this minimum has not been reached, bulk import jobs will refuse to start")
            .defaultProperty(DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT)
            .propertyGroup(TablePropertyGroup.BULK_IMPORT).build();

    // Ingest batcher
    TableProperty INGEST_BATCHER_MIN_JOB_SIZE = Index.propertyBuilder("sleeper.table.ingest.batcher.job.min.size")
            .defaultProperty(DEFAULT_INGEST_BATCHER_MIN_JOB_SIZE)
            .description("Specifies the minimum total file size required for an ingest job to be batched and sent. " +
                    "An ingest job will be created if the batcher runs while this much data is waiting, and the " +
                    "minimum number of files is also met.")
            .propertyGroup(TablePropertyGroup.INGEST_BATCHER).build();
    TableProperty INGEST_BATCHER_MAX_JOB_SIZE = Index.propertyBuilder("sleeper.table.ingest.batcher.job.max.size")
            .defaultProperty(DEFAULT_INGEST_BATCHER_MAX_JOB_SIZE)
            .description("Specifies the maximum total file size for a job in the ingest batcher. " +
                    "If more data is waiting than this, it will be split into multiple jobs. " +
                    "If a single file exceeds this, it will still be ingested in its own job. " +
                    "It's also possible some data may be left for a future run of the batcher if some recent files " +
                    "overflow the size of a job but aren't enough to create a job on their own.")
            .propertyGroup(TablePropertyGroup.INGEST_BATCHER).build();
    TableProperty INGEST_BATCHER_MIN_JOB_FILES = Index.propertyBuilder("sleeper.table.ingest.batcher.job.min.files")
            .defaultProperty(DEFAULT_INGEST_BATCHER_MIN_JOB_FILES)
            .description("Specifies the minimum number of files for a job in the ingest batcher. " +
                    "An ingest job will be created if the batcher runs while this many files are waiting, and the " +
                    "minimum size of files is also met.")
            .propertyGroup(TablePropertyGroup.INGEST_BATCHER).build();
    TableProperty INGEST_BATCHER_MAX_JOB_FILES = Index.propertyBuilder("sleeper.table.ingest.batcher.job.max.files")
            .defaultProperty(DEFAULT_INGEST_BATCHER_MAX_JOB_FILES)
            .description("Specifies the maximum number of files for a job in the ingest batcher. " +
                    "If more files are waiting than this, they will be split into multiple jobs. " +
                    "It's possible some data may be left for a future run of the batcher if some recent files " +
                    "overflow the size of a job but aren't enough to create a job on their own.")
            .propertyGroup(TablePropertyGroup.INGEST_BATCHER).build();
    TableProperty INGEST_BATCHER_MAX_FILE_AGE_SECONDS = Index.propertyBuilder("sleeper.table.ingest.batcher.file.max.age.seconds")
            .defaultProperty(DEFAULT_INGEST_BATCHER_MAX_FILE_AGE_SECONDS)
            .description("Specifies the maximum time in seconds that a file can be held in the batcher before it " +
                    "will be included in an ingest job. When any file has been waiting for longer than this, a job " +
                    "will be created with all the currently held files, even if other criteria for a batch are not " +
                    "met.")
            .propertyGroup(TablePropertyGroup.INGEST_BATCHER).build();
    TableProperty INGEST_BATCHER_INGEST_MODE = Index.propertyBuilder("sleeper.table.ingest.batcher.ingest.mode")
            .defaultProperty(DEFAULT_INGEST_BATCHER_INGEST_MODE)
            .description("Specifies the target ingest queue where batched jobs are sent.\n" +
                    "Valid values are: " + describeEnumValuesInLowerCase(BatchIngestMode.class))
            .propertyGroup(TablePropertyGroup.INGEST_BATCHER).build();
    TableProperty INGEST_BATCHER_TRACKING_TTL_MINUTES = Index.propertyBuilder("sleeper.table.ingest.batcher.file.tracking.ttl.minutes")
            .defaultProperty(DEFAULT_INGEST_BATCHER_TRACKING_TTL_MINUTES)
            .description("The time in minutes that the tracking information is retained for a file before the " +
                    "records of its ingest are deleted (eg. which ingest job it was assigned to, the time this " +
                    "occurred, the size of the file).\n" +
                    "The expiry time is fixed when a file is saved to the store, so changing this will only affect " +
                    "new data.\n" +
                    "Defaults to 1 week.")
            .propertyGroup(TablePropertyGroup.INGEST_BATCHER).build();

    // Size ratio compaction strategy
    TableProperty SIZE_RATIO_COMPACTION_STRATEGY_RATIO = Index.propertyBuilder("sleeper.table.compaction.strategy.sizeratio.ratio")
            .defaultProperty(DEFAULT_SIZERATIO_COMPACTION_STRATEGY_RATIO)
            .description("Used by the SizeRatioCompactionStrategy to decide if a group of files should be compacted.\n" +
                    "If the file sizes are s_1, ..., s_n then the files are compacted if s_1 + ... + s_{n-1} >= ratio * s_n.")
            .propertyGroup(TablePropertyGroup.COMPACTION)
            .build();
    TableProperty SIZE_RATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION = Index.propertyBuilder("sleeper.table.compaction.strategy.sizeratio.max.concurrent.jobs.per.partition")
            .defaultProperty(DEFAULT_SIZERATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION)
            .description("Used by the SizeRatioCompactionStrategy to control the maximum number of jobs that can be running " +
                    "concurrently per partition.")
            .propertyGroup(TablePropertyGroup.COMPACTION)
            .build();

    // System defined
    TableProperty SPLIT_POINTS_KEY = Index.propertyBuilder("sleeper.table.splits.key")
            .description("The key of the S3 object in the config bucket that defines initial split points for the table.")
            .propertyGroup(TablePropertyGroup.PARTITION_SPLITTING)
            .systemDefined(true).build();
    TableProperty DATA_BUCKET = Index.propertyBuilder("sleeper.table.data.bucket")
            .description("The S3 bucket name where table data is stored.")
            .propertyGroup(TablePropertyGroup.DATA_STORAGE)
            .systemDefined(true).build();
    // DynamoDBStateStore properties
    TableProperty ACTIVE_FILEINFO_TABLENAME = Index.propertyBuilder("sleeper.table.metadata.dynamo.active.table")
            .description("The name of the DynamoDB table holding metadata of active files in the Sleeper table.")
            .propertyGroup(TablePropertyGroup.METADATA)
            .systemDefined(true).build();
    TableProperty READY_FOR_GC_FILEINFO_TABLENAME = Index.propertyBuilder("sleeper.table.metadata.dynamo.gc.table")
            .description("The name of the DynamoDB table holding metadata of files ready for garbage collection " +
                    "in the Sleeper table.")
            .propertyGroup(TablePropertyGroup.METADATA)
            .systemDefined(true).build();
    TableProperty PARTITION_TABLENAME = Index.propertyBuilder("sleeper.table.metadata.dynamo.partition.table")
            .description("The name of the DynamoDB table holding metadata of partitions in the Sleeper table.")
            .propertyGroup(TablePropertyGroup.METADATA)
            .systemDefined(true).build();
    TableProperty DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY = Index.propertyBuilder("sleeper.table.metadata.dynamo.pointintimerecovery")
            .defaultProperty(DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY_ENABLED)
            .description("This specifies whether point in time recovery is enabled for DynanmoDB tables if " +
                    "the DynamoDBStateStore is used.")
            .propertyGroup(TablePropertyGroup.METADATA)
            .runCDKDeployWhenChanged(true)
            .systemDefined(true).build();
    // S3StateStore properties
    TableProperty REVISION_TABLENAME = Index.propertyBuilder("sleeper.table.metadata.s3.dynamo.revision.table")
            .description("The name of the DynamoDB table used for atomically updating the S3StateStore.")
            .propertyGroup(TablePropertyGroup.METADATA)
            .systemDefined(true).build();

    static List<TableProperty> getAll() {
        return Index.INSTANCE.getAll();
    }

    static List<TableProperty> getSystemDefined() {
        return Index.INSTANCE.getSystemDefined();
    }

    static List<TableProperty> getUserDefined() {
        return Index.INSTANCE.getUserDefined();
    }

    static boolean has(String propertyName) {
        return Index.INSTANCE.getByName(propertyName).isPresent();
    }


    class Index {
        private Index() {
        }

        static final SleeperPropertyIndex<TableProperty> INSTANCE = new SleeperPropertyIndex<>();

        private static TablePropertyImpl.Builder propertyBuilder(String propertyName) {
            return TablePropertyImpl.named(propertyName)
                    .addToIndex(INSTANCE::add);
        }
    }

    SleeperProperty getDefaultProperty();

    PropertyGroup getPropertyGroup();
}
