/*
 * Copyright 2022-2024 Crown Copyright
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
import sleeper.configuration.properties.SleeperPropertyIndex;
import sleeper.configuration.properties.instance.SleeperProperty;
import sleeper.configuration.properties.validation.IngestFileWritingStrategy;
import sleeper.configuration.properties.validation.IngestQueue;

import java.util.List;
import java.util.Objects;

import static sleeper.configuration.Utils.describeEnumValuesInLowerCase;
import static sleeper.configuration.properties.instance.CompactionProperty.DEFAULT_COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.instance.CompactionProperty.DEFAULT_COMPACTION_JOB_SEND_BATCH_SIZE;
import static sleeper.configuration.properties.instance.CompactionProperty.DEFAULT_COMPACTION_METHOD;
import static sleeper.configuration.properties.instance.CompactionProperty.DEFAULT_COMPACTION_STRATEGY_CLASS;
import static sleeper.configuration.properties.instance.CompactionProperty.DEFAULT_SIZERATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION;
import static sleeper.configuration.properties.instance.CompactionProperty.DEFAULT_SIZERATIO_COMPACTION_STRATEGY_RATIO;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_ADD_TRANSACTION_FIRST_RETRY_WAIT_CEILING_MS;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_ADD_TRANSACTION_MAX_ATTEMPTS;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_ADD_TRANSACTION_MAX_RETRY_WAIT_CEILING_MS;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_BULK_IMPORT_FILES_COMMIT_ASYNC;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_COMPACTION_JOB_COMMIT_ASYNC;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_COMPRESSION_CODEC;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_DICTIONARY_ENCODING_FOR_VALUE_FIELDS;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_DYNAMO_STRONGLY_CONSISTENT_READS;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_BATCHER_INGEST_QUEUE;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_BATCHER_MAX_FILE_AGE_SECONDS;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_BATCHER_MAX_JOB_FILES;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_BATCHER_MAX_JOB_SIZE;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_BATCHER_MIN_JOB_FILES;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_BATCHER_MIN_JOB_SIZE;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_BATCHER_TRACKING_TTL_MINUTES;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_FILES_COMMIT_ASYNC;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_FILE_WRITING_STRATEGY;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_RECORD_BATCH_TYPE;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_MIN_TRANSACTIONS_AHEAD_TO_LOAD_SNAPSHOT;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_PAGE_SIZE;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_PARQUET_WRITER_VERSION;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_ROW_GROUP_SIZE;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_S3A_READAHEAD_RANGE;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_STATISTICS_TRUNCATE_LENGTH;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_TIME_BETWEEN_SNAPSHOT_CHECKS_SECS;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_TIME_BETWEEN_TRANSACTION_CHECKS_MS;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_TRANSACTION_LOG_SNAPSHOT_MIN_AGE_MINUTES_TO_DELETE_TRANSACTIONS;
import static sleeper.configuration.properties.instance.GarbageCollectionProperty.DEFAULT_GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_EXECUTOR_ARM_INSTANCE_TYPES;
import static sleeper.configuration.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE;
import static sleeper.configuration.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES;
import static sleeper.configuration.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY;
import static sleeper.configuration.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_INSTANCE_ARCHITECTURE;
import static sleeper.configuration.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_MASTER_ARM_INSTANCE_TYPES;
import static sleeper.configuration.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES;
import static sleeper.configuration.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY;
import static sleeper.configuration.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL;
import static sleeper.configuration.properties.instance.PartitionSplittingProperty.DEFAULT_PARTITION_SPLIT_THRESHOLD;
import static sleeper.configuration.properties.instance.QueryProperty.DEFAULT_QUERY_PROCESSOR_CACHE_TIMEOUT;

/**
 * These contain the table properties which are stored separately to the instance properties.
 */
// Suppress as this class will always be referenced before impl class, so initialization behavior will be deterministic
@SuppressFBWarnings("IC_SUPERCLASS_USES_SUBCLASS_DURING_INITIALIZATION")
public interface TableProperty extends SleeperProperty {
    // User defined
    TableProperty TABLE_NAME = Index.propertyBuilder("sleeper.table.name")
            .validationPredicate(Objects::nonNull)
            .description("A unique name identifying this table.")
            .propertyGroup(TablePropertyGroup.DATA_DEFINITION)
            .editable(false).build();
    TableProperty TABLE_ID = Index.propertyBuilder("sleeper.table.id")
            .description("A unique ID identifying this table, generated by Sleeper on table creation.")
            .propertyGroup(TablePropertyGroup.DATA_DEFINITION)
            .setBySleeper(true).build();
    TableProperty TABLE_ONLINE = Index.propertyBuilder("sleeper.table.online")
            .description("A boolean flag representing whether this table is online or offline.\n" +
                    "An offline table will not have any partition splitting or compaction jobs run automatically.\n" +
                    "Note that taking a table offline will not stop any partitions that are being split or compaction " +
                    "jobs that are running. Additionally, you are still able to ingest data to offline tables and perform " +
                    "queries against them.")
            .propertyGroup(TablePropertyGroup.DATA_DEFINITION)
            .validationPredicate(Utils::isTrueOrFalse)
            .defaultValue("true")
            .setBySleeper(false).build();
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
    TableProperty SPLIT_POINTS_FILE = Index.propertyBuilder("sleeper.table.splits.file")
            .description("Splits file which will be used to initialise the partitions for this table. Defaults to nothing and the " +
                    "table will be created with a single root partition.")
            .propertyGroup(TablePropertyGroup.PARTITION_SPLITTING)
            .build();
    TableProperty SPLIT_POINTS_BASE64_ENCODED = Index.propertyBuilder("sleeper.table.splits.base64.encoded")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse)
            .description("Flag to set if you have base64 encoded the split points (only used for string key types and defaults to false).")
            .propertyGroup(TablePropertyGroup.PARTITION_SPLITTING)
            .build();
    TableProperty PARTITION_SPLIT_THRESHOLD = Index.propertyBuilder("sleeper.table.partition.splitting.threshold")
            .defaultProperty(DEFAULT_PARTITION_SPLIT_THRESHOLD)
            .description("Partitions in this table with more than the following number of records in will be split.")
            .propertyGroup(TablePropertyGroup.PARTITION_SPLITTING)
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
    TableProperty COLUMN_INDEX_TRUNCATE_LENGTH = Index.propertyBuilder("sleeper.table.parquet.columnindex.truncate.length")
            .defaultProperty(DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH)
            .description("Used to set parquet.columnindex.truncate.length, see documentation here:\n" +
                    "https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/README.md\n" +
                    "The length in bytes to truncate binary values in a column index.")
            .propertyGroup(TablePropertyGroup.DATA_STORAGE).build();
    TableProperty STATISTICS_TRUNCATE_LENGTH = Index.propertyBuilder("sleeper.table.parquet.statistics.truncate.length")
            .defaultProperty(DEFAULT_STATISTICS_TRUNCATE_LENGTH)
            .description("Used to set parquet.statistics.truncate.length, see documentation here:\n" +
                    "https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/README.md\n" +
                    "The length in bytes to truncate the min/max binary values in row groups.")
            .propertyGroup(TablePropertyGroup.DATA_STORAGE).build();
    TableProperty PARQUET_WRITER_VERSION = Index.propertyBuilder("sleeper.table.parquet.writer.version")
            .description("Used to set parquet.writer.version, see documentation here:\n" +
                    "https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/README.md\n" +
                    "Can be either v1 or v2. The v2 pages store levels uncompressed while v1 pages compress levels with the data.")
            .defaultProperty(DEFAULT_PARQUET_WRITER_VERSION)
            .propertyGroup(TablePropertyGroup.DATA_STORAGE).build();
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
    TableProperty GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION = Index.propertyBuilder("sleeper.table.gc.delay.minutes")
            .defaultProperty(DEFAULT_GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION)
            .description("A file will not be deleted until this number of minutes have passed after it has been marked as ready for " +
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
            .description("The maximum number of files to read in a compaction job. Note that the state store must " +
                    "support atomic updates for this many files.\n" +
                    "The DynamoDBStateStore must be able to atomically apply 2 updates for each input file to remove " +
                    "the file references and update the file reference count, and another 2 updates for an output file " +
                    "to add a new file reference and update the reference count. There's a limit of 100 atomic updates, " +
                    "which equates to 49 files in a compaction.\n" +
                    "See also: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html.\n" +
                    "Also note that this many files may need to be open simultaneously. The value of " +
                    "'sleeper.fs.s3a.max-connections' must be at least the value of this plus one. The extra one is " +
                    "for the output file.")
            .propertyGroup(TablePropertyGroup.COMPACTION)
            .build();
    TableProperty COMPACTION_JOB_SEND_BATCH_SIZE = Index.propertyBuilder("sleeper.table.compaction.job.send.batch.size")
            .defaultProperty(DEFAULT_COMPACTION_JOB_SEND_BATCH_SIZE)
            .description("The number of compaction jobs to send in a single batch.\n" +
                    "When compaction jobs are created, there is no limit on how many jobs can be created at once. " +
                    "A batch is a group of compaction jobs that will have their creation updates applied at the same time. " +
                    "For each batch, we send all compaction jobs to the SQS queue, then update the state store to " +
                    "assign job IDs to the input files.")
            .propertyGroup(TablePropertyGroup.COMPACTION)
            .build();
    TableProperty COMPACTION_JOB_COMMIT_ASYNC = Index.propertyBuilder("sleeper.table.compaction.job.commit.async")
            .defaultProperty(DEFAULT_COMPACTION_JOB_COMMIT_ASYNC)
            .description("If true, compaction job commit requests will be sent to the state store committer lambda " +
                    "to be performed asynchronously. If false, compaction jobs will be committed synchronously by compaction tasks.")
            .propertyGroup(TablePropertyGroup.COMPACTION)
            .build();
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

    TableProperty COMPACTION_METHOD = Index.propertyBuilder("sleeper.table.compaction.method")
            .defaultProperty(DEFAULT_COMPACTION_METHOD)
            .description("Select what compaction method to use on a table. Current options are JAVA, RUST and GPU. Rust and GPU compaction support are " +
                    "experimental.")
            .propertyGroup(TablePropertyGroup.COMPACTION)
            .build();

    TableProperty STATESTORE_CLASSNAME = Index.propertyBuilder("sleeper.table.statestore.classname")
            .defaultValue("sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore")
            .description("The name of the class used for the state store. " +
                    "The default is DynamoDBTransactionLogStateStore. Options are:\n" +
                    "sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore\n" +
                    "sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStoreNoSnapshots\n" +
                    "sleeper.statestore.s3.S3StateStore\n" +
                    "sleeper.statestore.dynamodb.DynamoDBStateStore")
            .propertyGroup(TablePropertyGroup.METADATA)
            .editable(false).build();
    TableProperty ADD_TRANSACTION_MAX_ATTEMPTS = Index.propertyBuilder("sleeper.table.statestore.transactionlog.add.transaction.max.attempts")
            .defaultProperty(DEFAULT_ADD_TRANSACTION_MAX_ATTEMPTS)
            .description("The number of attempts to make when applying a transaction to the state store.")
            .propertyGroup(TablePropertyGroup.METADATA)
            .build();
    TableProperty ADD_TRANSACTION_FIRST_RETRY_WAIT_CEILING_MS = Index.propertyBuilder("sleeper.table.statestore.transactionlog.add.transaction.first.retry.wait.ceiling.ms")
            .defaultProperty(DEFAULT_ADD_TRANSACTION_FIRST_RETRY_WAIT_CEILING_MS)
            .description("The maximum amount of time to wait before the first retry when applying a transaction to " +
                    "the state store. Full jitter will be applied so that the actual wait time will be a random " +
                    "period between 0 and this value. This ceiling will increase exponentially on further retries. " +
                    "See the below article.\n" +
                    "https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/")
            .propertyGroup(TablePropertyGroup.METADATA)
            .build();
    TableProperty ADD_TRANSACTION_MAX_RETRY_WAIT_CEILING_MS = Index.propertyBuilder("sleeper.table.statestore.transactionlog.add.transaction.max.retry.wait.ceiling.ms")
            .defaultProperty(DEFAULT_ADD_TRANSACTION_MAX_RETRY_WAIT_CEILING_MS)
            .description("The maximum amount of time to wait before any retry when applying a transaction to " +
                    "the state store. Full jitter will be applied so that the actual wait time will be a random " +
                    "period between 0 and this value. This restricts the exponential increase of the wait ceiling " +
                    "while retrying the transaction. See the below article.\n" +
                    "https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/")
            .propertyGroup(TablePropertyGroup.METADATA)
            .build();
    TableProperty TIME_BETWEEN_SNAPSHOT_CHECKS_SECS = Index.propertyBuilder("sleeper.table.statestore.transactionlog.time.between.snapshot.checks.secs")
            .defaultProperty(DEFAULT_TIME_BETWEEN_SNAPSHOT_CHECKS_SECS)
            .description("The number of seconds to wait after we've loaded a snapshot before looking for a new " +
                    "snapshot. This should relate to the rate at which new snapshots are created, configured in the " +
                    "instance property `sleeper.statestore.transactionlog.snapshot.creation.lambda.period.minutes`.")
            .propertyGroup(TablePropertyGroup.METADATA)
            .build();
    TableProperty TIME_BETWEEN_TRANSACTION_CHECKS_MS = Index.propertyBuilder("sleeper.table.statestore.transactionlog.time.between.transaction.checks.ms")
            .defaultProperty(DEFAULT_TIME_BETWEEN_TRANSACTION_CHECKS_MS)
            .description("The number of milliseconds to wait after we've updated from the transaction log before " +
                    "checking for new transactions. The state visible to an instance of the state store can be out " +
                    "of date by this amount. This can avoid excessive queries by the same process, but can result in " +
                    "unwanted behaviour when using multiple state store objects. When adding a new transaction to " +
                    "update the state, this will be ignored and the state will be brought completely up to date.")
            .propertyGroup(TablePropertyGroup.METADATA)
            .build();
    TableProperty MIN_TRANSACTIONS_AHEAD_TO_LOAD_SNAPSHOT = Index.propertyBuilder("sleeper.table.statestore.transactionlog.snapshot.load.min.transactions.ahead")
            .defaultProperty(DEFAULT_MIN_TRANSACTIONS_AHEAD_TO_LOAD_SNAPSHOT)
            .description("The minimum number of transactions that a snapshot must be ahead of the local " +
                    "state, before we load the snapshot instead of updating from the transaction log.")
            .propertyGroup(TablePropertyGroup.METADATA)
            .build();
    TableProperty TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS = Index.propertyBuilder("sleeper.table.statestore.transactionlog.snapshot.expiry.days")
            .defaultProperty(DEFAULT_TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS)
            .description("The number of days that transaction log snapshots remain in the snapshot store before being deleted.")
            .propertyGroup(TablePropertyGroup.METADATA)
            .build();
    TableProperty TRANSACTION_LOG_SNAPSHOT_MIN_AGE_MINUTES_TO_DELETE_TRANSACTIONS = Index.propertyBuilder("sleeper.table.statestore.transactionlog.delete.behind.snapshot.min.age.minutes")
            .description("The minimum age in minutes of a snapshot in order to allow deletion of transactions " +
                    "leading up to it. When deleting old transactions, there's a chance that processes may still " +
                    "read transactions starting from an older snapshot. We need to avoid deletion of any " +
                    "transactions associated with a snapshot that may still be used as the starting point for " +
                    "reading the log.")
            .defaultProperty(DEFAULT_TRANSACTION_LOG_SNAPSHOT_MIN_AGE_MINUTES_TO_DELETE_TRANSACTIONS)
            .propertyGroup(TablePropertyGroup.METADATA)
            .build();
    TableProperty TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE = Index.propertyBuilder("sleeper.table.statestore.transactionlog.delete.number.behind.latest.snapshot")
            .defaultProperty(DEFAULT_TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE)
            .description("The minimum number of transactions that a transaction must be behind the latest snapshot " +
                    "before being deleted. This is the number of transactions that will be kept and protected from " +
                    "deletion, whenever old transactions are deleted. This includes the transaction that the latest " +
                    "snapshot was created against. Any transactions after the snapshot will never be deleted as they " +
                    "are still in active use.\n" +
                    "This should be configured in relation to the property which determines whether a process will " +
                    "load the latest snapshot or instead seek through the transaction log, since we need to preserve " +
                    "transactions that may still be read:\n" +
                    "sleeper.table.statestore.snapshot.load.min.transactions.ahead\n" +
                    "The snapshot that will be considered the latest snapshot is configured by a property to set the " +
                    "minimum age for it to count for this:\n" +
                    "sleeper.table.statestore.transactionlog.delete.behind.snapshot.min.age\n")
            .propertyGroup(TablePropertyGroup.METADATA)
            .build();
    TableProperty DYNAMODB_STRONGLY_CONSISTENT_READS = Index.propertyBuilder("sleeper.table.statestore.dynamo.consistent.reads")
            .defaultProperty(DEFAULT_DYNAMO_STRONGLY_CONSISTENT_READS)
            .description("This specifies whether queries and scans against DynamoDB tables used in the state stores " +
                    "are strongly consistent.")
            .propertyGroup(TablePropertyGroup.METADATA)
            .build();
    TableProperty BULK_IMPORT_EMR_INSTANCE_ARCHITECTURE = Index.propertyBuilder("sleeper.table.bulk.import.emr.instance.architecture")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_INSTANCE_ARCHITECTURE)
            .description("(Non-persistent EMR mode only) Which architecture to be used for EC2 instance types " +
                    "in the EMR cluster. Must be either \"x86_64\" \"arm64\" or \"x86_64,arm64\". " +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .propertyGroup(TablePropertyGroup.BULK_IMPORT)
            .build();
    TableProperty BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES = Index.propertyBuilder("sleeper.table.bulk.import.emr.master.x86.instance.types")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES)
            .description("(Non-persistent EMR mode only) The EC2 x86_64 instance types and weights to be used for " +
                    "the master node of the EMR cluster.\n" +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .propertyGroup(TablePropertyGroup.BULK_IMPORT)
            .build();
    TableProperty BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES = Index.propertyBuilder("sleeper.table.bulk.import.emr.executor.x86.instance.types")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES)
            .description("(Non-persistent EMR mode only) The EC2 x86_64 instance types and weights to be used for " +
                    "the executor nodes of the EMR cluster.\n" +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .propertyGroup(TablePropertyGroup.BULK_IMPORT)
            .build();
    TableProperty BULK_IMPORT_EMR_MASTER_ARM_INSTANCE_TYPES = Index.propertyBuilder("sleeper.table.bulk.import.emr.master.arm.instance.types")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_MASTER_ARM_INSTANCE_TYPES)
            .description("(Non-persistent EMR mode only) The EC2 ARM64 instance types and weights to be used for the " +
                    "master node of the EMR cluster.\n" +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .propertyGroup(TablePropertyGroup.BULK_IMPORT)
            .build();
    TableProperty BULK_IMPORT_EMR_EXECUTOR_ARM_INSTANCE_TYPES = Index.propertyBuilder("sleeper.table.bulk.import.emr.executor.arm.instance.types")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_EXECUTOR_ARM_INSTANCE_TYPES)
            .description("(Non-persistent EMR mode only) The EC2 ARM64 instance types and weights to be used for the " +
                    "executor nodes of the EMR cluster.\n" +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .propertyGroup(TablePropertyGroup.BULK_IMPORT)
            .build();
    TableProperty BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE = Index.propertyBuilder("sleeper.table.bulk.import.emr.executor.market.type")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE)
            .description("(Non-persistent EMR mode only) The purchasing option to be used for the executor nodes of " +
                    "the EMR cluster.\n" +
                    "Valid values are ON_DEMAND or SPOT.")
            .propertyGroup(TablePropertyGroup.BULK_IMPORT)
            .build();
    TableProperty BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY = Index.propertyBuilder("sleeper.table.bulk.import.emr.executor.initial.capacity")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY)
            .description("(Non-persistent EMR mode only) The initial number of capacity units to provision as EC2 " +
                    "instances for executors in the EMR cluster.\n" +
                    "This is measured in instance fleet capacity units. These are declared alongside the requested " +
                    "instance types, as each type will count for a certain number of units. By default the units are " +
                    "the number of instances.\n" +
                    "This value overrides the default value in the instance properties. " +
                    "It can be overridden by a value in the bulk import job specification.")
            .propertyGroup(TablePropertyGroup.BULK_IMPORT)
            .build();
    TableProperty BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY = Index.propertyBuilder("sleeper.table.bulk.import.emr.executor.max.capacity")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY)
            .description("(Non-persistent EMR mode only) The maximum number of capacity units to provision as EC2 " +
                    "instances for executors in the EMR cluster.\n" +
                    "This is measured in instance fleet capacity units. These are declared alongside the requested " +
                    "instance types, as each type will count for a certain number of units. By default the units are " +
                    "the number of instances.\n" +
                    "This value overrides the default value in the instance properties. " +
                    "It can be overridden by a value in the bulk import job specification.")
            .propertyGroup(TablePropertyGroup.BULK_IMPORT)
            .build();
    TableProperty BULK_IMPORT_EMR_RELEASE_LABEL = Index.propertyBuilder("sleeper.table.bulk.import.emr.release.label")
            .defaultProperty(DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL)
            .description("(Non-persistent EMR mode only) The EMR release label to be used when creating an EMR " +
                    "cluster for bulk importing data using Spark running on EMR.\n" +
                    "This value overrides the default value in the instance properties. " +
                    "It can be overridden by a value in the bulk import job specification.")
            .propertyGroup(TablePropertyGroup.BULK_IMPORT)
            .build();
    TableProperty BULK_IMPORT_MIN_LEAF_PARTITION_COUNT = Index.propertyBuilder("sleeper.table.bulk.import.min.leaf.partitions")
            .description("Specifies the minimum number of leaf partitions that are needed to run a bulk import job. " +
                    "If this minimum has not been reached, bulk import jobs will refuse to start")
            .defaultProperty(DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT)
            .propertyGroup(TablePropertyGroup.BULK_IMPORT).build();
    TableProperty BULK_IMPORT_FILES_COMMIT_ASYNC = Index.propertyBuilder("sleeper.table.bulk.import.job.files.commit.async")
            .defaultProperty(DEFAULT_BULK_IMPORT_FILES_COMMIT_ASYNC)
            .description("If true, bulk import will add files via requests sent to the state store committer lambda " +
                    "asynchronously. If false, bulk import will commit new files at the end of the job synchronously.")
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
    TableProperty INGEST_BATCHER_INGEST_QUEUE = Index.propertyBuilder("sleeper.table.ingest.batcher.ingest.queue")
            .defaultProperty(DEFAULT_INGEST_BATCHER_INGEST_QUEUE)
            .description("Specifies the target ingest queue where batched jobs are sent.\n" +
                    "Valid values are: " + describeEnumValuesInLowerCase(IngestQueue.class))
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
    TableProperty QUERY_PROCESSOR_CACHE_TIMEOUT = Index.propertyBuilder("sleeper.table.query.processor.cache.timeout")
            .defaultProperty(DEFAULT_QUERY_PROCESSOR_CACHE_TIMEOUT)
            .description("The amount of time in minutes the query executor cache is valid for before it times out and needs refreshing.")
            .propertyGroup(TablePropertyGroup.QUERY_EXECUTION).build();
    TableProperty INGEST_FILE_WRITING_STRATEGY = Index.propertyBuilder("sleeper.table.ingest.file.writing.strategy")
            .defaultProperty(DEFAULT_INGEST_FILE_WRITING_STRATEGY)
            .description("Specifies the strategy that ingest uses to creates files and references in partitions.\n" +
                    "Valid values are: " + describeEnumValuesInLowerCase(IngestFileWritingStrategy.class))
            .propertyGroup(TablePropertyGroup.INGEST).build();
    TableProperty INGEST_RECORD_BATCH_TYPE = Index.propertyBuilder("sleeper.table.ingest.record.batch.type")
            .defaultProperty(DEFAULT_INGEST_RECORD_BATCH_TYPE)
            .description("The way in which records are held in memory before they are written to a local store.\n" +
                    "Valid values are 'arraylist' and 'arrow'.\n" +
                    "The arraylist method is simpler, but it is slower and requires careful tuning of the number of records in each batch.")
            .propertyGroup(TablePropertyGroup.INGEST).build();
    TableProperty INGEST_PARTITION_FILE_WRITER_TYPE = Index.propertyBuilder("sleeper.table.ingest.partition.file.writer.type")
            .defaultProperty(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE)
            .description("The way in which partition files are written to the main Sleeper store.\n" +
                    "Valid values are 'direct' (which writes using the s3a Hadoop file system) and 'async' (which writes locally and then " +
                    "copies the completed Parquet file asynchronously into S3).\n" +
                    "The direct method is simpler but the async method should provide better performance when the number of partitions " +
                    "is large.")
            .propertyGroup(TablePropertyGroup.INGEST).build();
    TableProperty INGEST_FILES_COMMIT_ASYNC = Index.propertyBuilder("sleeper.table.ingest.job.files.commit.async")
            .defaultProperty(DEFAULT_INGEST_FILES_COMMIT_ASYNC)
            .description("If true, ingest tasks will add files via requests sent to the state store committer lambda " +
                    "asynchronously. If false, ingest tasks will commit new files synchronously.")
            .propertyGroup(TablePropertyGroup.INGEST).build();

    static List<TableProperty> getAll() {
        return Index.INSTANCE.getAll();
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
