/*
 * Copyright 2022-2025 Crown Copyright
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

package sleeper.core.properties.instance;

import sleeper.core.properties.SleeperPropertyIndex;
import sleeper.core.properties.model.CompressionCodec;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.model.DefaultAsyncCommitBehaviour;
import sleeper.core.properties.model.IngestFileWritingStrategy;
import sleeper.core.properties.model.IngestQueue;
import sleeper.core.properties.model.SleeperPropertyValueUtils;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;

import java.util.List;
import java.util.Locale;

import static sleeper.core.properties.model.SleeperPropertyValueUtils.describeEnumValuesInLowerCase;

/**
 * Definitions of instance properties that are defaults for table properties.
 */
public interface TableDefaultProperty {
    UserDefinedInstanceProperty DEFAULT_PARQUET_QUERY_COLUMN_INDEX_ENABLED = Index.propertyBuilder("sleeper.default.table.parquet.query.column.index.enabled")
            .description("Used during Sleeper queries to determine whether the column/offset indexes (also known as page indexes) are read from Parquet files. " +
                    "For some queries, e.g. single/few row lookups this can improve performance by enabling more aggressive pruning. On range " +
                    "queries, especially on large tables this can harm performance, since readers will read the extra index data before " +
                    "returning results, but with little benefit from pruning.")
            .defaultValue("false")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_ROW_GROUP_SIZE = Index.propertyBuilder("sleeper.default.table.rowgroup.size")
            .description("Maximum number of bytes to write in a Parquet row group (default is 8MiB). " +
                    "This property is NOT used by DataFusion data engine.")
            .defaultValue("" + (8 * 1024 * 1024)) // 8 MiB
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_PAGE_SIZE = Index.propertyBuilder("sleeper.default.table.page.size")
            .description("The size of the pages in the Parquet files (default is 128KiB).")
            .defaultValue("" + (128 * 1024)) // 128 KiB
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_COMPRESSION_CODEC = Index.propertyBuilder("sleeper.default.table.compression.codec")
            .description("The compression codec to use in the Parquet files.\n" +
                    "Valid values are: " + describeEnumValuesInLowerCase(CompressionCodec.class))
            .defaultValue("zstd")
            .validationPredicate(CompressionCodec::isValid)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS = Index.propertyBuilder("sleeper.default.table.parquet.dictionary.encoding.rowkey.fields")
            .description("Whether dictionary encoding should be used for row key columns in the Parquet files.")
            .defaultValue("false")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS = Index.propertyBuilder("sleeper.default.table.parquet.dictionary.encoding.sortkey.fields")
            .description("Whether dictionary encoding should be used for sort key columns in the Parquet files.")
            .defaultValue("false")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_DICTIONARY_ENCODING_FOR_VALUE_FIELDS = Index.propertyBuilder("sleeper.default.table.parquet.dictionary.encoding.value.fields")
            .description("Whether dictionary encoding should be used for value columns in the Parquet files.")
            .defaultValue("false")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH = Index.propertyBuilder("sleeper.default.table.parquet.columnindex.truncate.length")
            .description("Used to set parquet.columnindex.truncate.length, see documentation here:\n" +
                    "https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/README.md\n" +
                    "The length in bytes to truncate binary values in a column index.")
            .defaultValue("128")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_STATISTICS_TRUNCATE_LENGTH = Index.propertyBuilder("sleeper.default.table.parquet.statistics.truncate.length")
            .description("Used to set parquet.statistics.truncate.length, see documentation here:\n" +
                    "https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/README.md\n" +
                    "The length in bytes to truncate the min/max binary values in row groups.")
            .defaultValue("2147483647")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_DATAFUSION_S3_READAHEAD_ENABLED = Index.propertyBuilder("sleeper.default.table.datafusion.s3.readahead.enabled")
            .description("Enables a cache of data when reading from S3 with the DataFusion data engine, to hold data " +
                    "in larger blocks than are requested by DataFusion.")
            .defaultValue("true")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_PARQUET_WRITER_VERSION = Index.propertyBuilder("sleeper.default.table.parquet.writer.version")
            .description("Used to set parquet.writer.version, see documentation here:\n" +
                    "https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/README.md\n" +
                    "Can be either v1 or v2. The v2 pages store levels uncompressed while v1 pages compress levels with the data.")
            .defaultValue("v2")
            .validationPredicate(List.of("v1", "v2")::contains)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_PARQUET_ROWGROUP_ROWS = Index.propertyBuilder("sleeper.default.table.parquet.rowgroup.rows.max")
            .description("Maximum number of rows to write in a Parquet row group.")
            .defaultValue("1000000")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_ADD_TRANSACTION_MAX_ATTEMPTS = Index.propertyBuilder("sleeper.default.table.statestore.transactionlog.add.transaction.max.attempts")
            .description("The number of attempts to make when applying a transaction to the state store. " +
                    "This default can be overridden by a table property.")
            .defaultValue("" + TransactionLogStateStore.DEFAULT_MAX_ADD_TRANSACTION_ATTEMPTS)
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_ADD_TRANSACTION_FIRST_RETRY_WAIT_CEILING_MS = Index
            .propertyBuilder("sleeper.default.table.statestore.transactionlog.add.transaction.first.retry.wait.ceiling.ms")
            .description("The maximum amount of time to wait before the first retry when applying a transaction to " +
                    "the state store. Full jitter will be applied so that the actual wait time will be a random " +
                    "period between 0 and this value. This ceiling will increase exponentially on further retries. " +
                    "See the below article.\n" +
                    "https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/\n" +
                    "This default can be overridden by a table property.")
            .defaultValue("" + TransactionLogStateStore.DEFAULT_RETRY_WAIT_RANGE.getFirstWaitCeiling().toMillis())
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_ADD_TRANSACTION_MAX_RETRY_WAIT_CEILING_MS = Index.propertyBuilder("sleeper.default.table.statestore.transactionlog.add.transaction.max.retry.wait.ceiling.ms")
            .description("The maximum amount of time to wait before any retry when applying a transaction to " +
                    "the state store. Full jitter will be applied so that the actual wait time will be a random " +
                    "period between 0 and this value. This restricts the exponential increase of the wait ceiling " +
                    "while retrying the transaction. See the below article.\n" +
                    "https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/\n" +
                    "This default can be overridden by a table property.")
            .defaultValue("" + TransactionLogStateStore.DEFAULT_RETRY_WAIT_RANGE.getMaxWaitCeiling().toMillis())
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_FILES_SNAPSHOT_BATCH_SIZE = Index.propertyBuilder("sleeper.default.table.statestore.transactionlog.files.snapshot.batch.size")
            .description("The number of elements to include per Arrow row batch in a snapshot derived from the " +
                    "transaction log, of the state of files in a Sleeper table. Each file includes some number of " +
                    "references on different partitions. Each reference will count for one element in a row " +
                    "batch, but a file cannot currently be split between row batches. A row batch may contain " +
                    "more file references than this if a single file overflows the batch. A file with no references " +
                    "counts as one element.")
            .defaultValue("1000")
            .validationPredicate(SleeperPropertyValueUtils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_PARTITIONS_SNAPSHOT_BATCH_SIZE = Index.propertyBuilder("sleeper.default.table.statestore.transactionlog.partitions.snapshot.batch.size")
            .description("The number of partitions to include per Arrow row batch in a snapshot derived from the " +
                    "transaction log, of the state of partitions in a Sleeper table.")
            .defaultValue("1000")
            .validationPredicate(SleeperPropertyValueUtils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_TIME_BETWEEN_SNAPSHOT_CHECKS_SECS = Index.propertyBuilder("sleeper.default.table.statestore.transactionlog.time.between.snapshot.checks.seconds")
            .description("The number of seconds to wait after we've loaded a snapshot before looking for a new " +
                    "snapshot. This should relate to the rate at which new snapshots are created, configured in the " +
                    "instance property `sleeper.statestore.transactionlog.snapshot.creation.lambda.period.seconds`. " +
                    "This default can be overridden by a table property.")
            .defaultValue("" + TransactionLogStateStore.DEFAULT_TIME_BETWEEN_SNAPSHOT_CHECKS.toSeconds())
            .validationPredicate(SleeperPropertyValueUtils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_TIME_BETWEEN_TRANSACTION_CHECKS_MS = Index.propertyBuilder("sleeper.default.table.statestore.transactionlog.time.between.transaction.checks.ms")
            .description("The number of milliseconds to wait after we've updated from the transaction log before " +
                    "checking for new transactions. The state visible to an instance of the state store can be out " +
                    "of date by this amount. This can avoid excessive queries by the same process, but can result in " +
                    "unwanted behaviour when using multiple state store objects. When adding a new transaction to " +
                    "update the state, this will be ignored and the state will be brought completely up to date. " +
                    "This default can be overridden by a table property.")
            .defaultValue("" + TransactionLogStateStore.DEFAULT_TIME_BETWEEN_TRANSACTION_CHECKS.toMillis())
            .validationPredicate(SleeperPropertyValueUtils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_MIN_TRANSACTIONS_AHEAD_TO_LOAD_SNAPSHOT = Index.propertyBuilder("sleeper.default.table.statestore.transactionlog.snapshot.load.min.transactions.ahead")
            .description("The minimum number of transactions that a snapshot must be ahead of the local " +
                    "state, before we load the snapshot instead of updating from the transaction log.")
            .defaultValue("" + TransactionLogStateStore.DEFAULT_MIN_TRANSACTIONS_AHEAD_TO_LOAD_SNAPSHOT)
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS = Index.propertyBuilder("sleeper.default.table.statestore.transactionlog.snapshot.expiry.days")
            .description("The number of days that transaction log snapshots remain in the snapshot store before being deleted.")
            .defaultValue("2")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT)
            .build();
    UserDefinedInstanceProperty DEFAULT_TRANSACTION_LOG_SNAPSHOT_MIN_AGE_MINUTES_TO_DELETE_TRANSACTIONS = Index
            .propertyBuilder("sleeper.default.table.statestore.transactionlog.delete.behind.snapshot.min.age.minutes")
            .description("The minimum age in minutes of a snapshot in order to allow deletion of transactions " +
                    "leading up to it. When deleting old transactions, there's a chance that processes may still " +
                    "read transactions starting from an older snapshot. We need to avoid deletion of any " +
                    "transactions associated with a snapshot that may still be used as the starting point for " +
                    "reading the log.")
            .defaultValue("2")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT)
            .build();
    UserDefinedInstanceProperty DEFAULT_TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE = Index.propertyBuilder("sleeper.default.table.statestore.transactionlog.delete.number.behind.latest.snapshot")
            .description("The minimum number of transactions that a transaction must be behind the latest snapshot " +
                    "before being deleted. This is the number of transactions that will be kept and protected from " +
                    "deletion, whenever old transactions are deleted. This includes the transaction that the latest " +
                    "snapshot was created against. Any transactions after the snapshot will never be deleted as they " +
                    "are still in active use.\n" +
                    "This should be configured in relation to the property which determines whether a process will " +
                    "load the latest snapshot or instead seek through the transaction log, since we need to preserve " +
                    "transactions that may still be read:\n" +
                    "sleeper.default.statestore.snapshot.load.min.transactions.ahead\n" +
                    "The snapshot that will be considered the latest snapshot is configured by a property to set the " +
                    "minimum age for it to count for this:\n" +
                    "sleeper.default.statestore.transactionlog.delete.behind.snapshot.min.age\n")
            .defaultValue("200")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT)
            .build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT = Index.propertyBuilder("sleeper.default.table.bulk.import.min.leaf.partitions")
            .description("Specifies the minimum number of leaf partitions that are needed to run a bulk import job. " +
                    "If this minimum has not been reached, bulk import jobs will refuse to start.")
            .defaultValue("256")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();

    UserDefinedInstanceProperty DEFAULT_INGEST_BATCHER_MIN_JOB_SIZE = Index.propertyBuilder("sleeper.default.table.ingest.batcher.job.min.size")
            .description("Specifies the minimum total file size required for an ingest job to be batched and sent. " +
                    "An ingest job will be created if the batcher runs while this much data is waiting, and the " +
                    "minimum number of files is also met.")
            .defaultValue("1G")
            .validationPredicate(SleeperPropertyValueUtils::isValidNumberOfBytes)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_INGEST_BATCHER_MAX_JOB_SIZE = Index.propertyBuilder("sleeper.default.table.ingest.batcher.job.max.size")
            .description("Specifies the maximum total file size for a job in the ingest batcher. " +
                    "If more data is waiting than this, it will be split into multiple jobs. " +
                    "If a single file exceeds this, it will still be ingested in its own job. " +
                    "It's also possible some data may be left for a future run of the batcher if some recent files " +
                    "overflow the size of a job but aren't enough to create a job on their own.")
            .defaultValue("5G")
            .validationPredicate(SleeperPropertyValueUtils::isValidNumberOfBytes)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_INGEST_BATCHER_MIN_JOB_FILES = Index.propertyBuilder("sleeper.default.table.ingest.batcher.job.min.files")
            .description("Specifies the minimum number of files for a job in the ingest batcher. " +
                    "An ingest job will be created if the batcher runs while this many files are waiting, and the " +
                    "minimum size of files is also met.")
            .defaultValue("1")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_INGEST_BATCHER_MAX_JOB_FILES = Index.propertyBuilder("sleeper.default.table.ingest.batcher.job.max.files")
            .description("Specifies the maximum number of files for a job in the ingest batcher. " +
                    "If more files are waiting than this, they will be split into multiple jobs. " +
                    "It's possible some data may be left for a future run of the batcher if some recent files " +
                    "overflow the size of a job but aren't enough to create a job on their own.")
            .defaultValue("100")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_INGEST_BATCHER_MAX_FILE_AGE_SECONDS = Index.propertyBuilder("sleeper.default.table.ingest.batcher.file.max.age.seconds")
            .description("Specifies the maximum time in seconds that a file can be held in the batcher before it " +
                    "will be included in an ingest job. When any file has been waiting for longer than this, jobs " +
                    "will be created for all the currently held files, even if other criteria for a batch are not " +
                    "met.")
            .defaultValue("300")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_INGEST_BATCHER_INGEST_QUEUE = Index.propertyBuilder("sleeper.default.table.ingest.batcher.ingest.queue")
            .description("Specifies the target ingest queue where batched jobs are sent.\n" +
                    "Valid values are: " + describeEnumValuesInLowerCase(IngestQueue.class))
            .defaultValue(IngestQueue.BULK_IMPORT_EMR_SERVERLESS.name().toLowerCase(Locale.ROOT))
            .validationPredicate(IngestQueue::isValid)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_INGEST_BATCHER_TRACKING_TTL_MINUTES = Index.propertyBuilder("sleeper.default.table.ingest.batcher.file.tracking.ttl.minutes")
            .description("The time in minutes that the tracking information is retained for a file before the " +
                    "records of its ingest are deleted (eg. which ingest job it was assigned to, the time this " +
                    "occurred, the size of the file).\n" +
                    "The expiry time is fixed when a file is saved to the store, so changing this will only affect " +
                    "new data.\n" +
                    "Defaults to 1 week.")
            .defaultValue("" + 60 * 24 * 7)
            .validationPredicate(SleeperPropertyValueUtils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_INGEST_FILE_WRITING_STRATEGY = Index.propertyBuilder("sleeper.default.table.ingest.file.writing.strategy")
            .description("Specifies the strategy that ingest uses to create files and references in partitions.\n" +
                    "Valid values are: " + describeEnumValuesInLowerCase(IngestFileWritingStrategy.class))
            .defaultValue(IngestFileWritingStrategy.ONE_REFERENCE_PER_LEAF.name().toLowerCase(Locale.ROOT))
            .validationPredicate(IngestFileWritingStrategy::isValid)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_INGEST_ROW_BATCH_TYPE = Index.propertyBuilder("sleeper.default.table.ingest.row.batch.type")
            .description("The way in which rows are held in memory before they are written to a local store.\n" +
                    "Valid values are 'arraylist' and 'arrow'.\n" +
                    "The arraylist method is simpler, but it is slower and requires careful tuning of the number of rows in each batch.")
            .defaultValue("arrow")
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE = Index.propertyBuilder("sleeper.default.table.ingest.partition.file.writer.type")
            .description("The way in which partition files are written to the main Sleeper store.\n" +
                    "Valid values are 'direct' (which writes using the s3a Hadoop file system) and 'async' (which writes locally and then " +
                    "copies the completed Parquet file asynchronously into S3).\n" +
                    "The direct method is simpler but the async method should provide better performance when the number of partitions " +
                    "is large.")
            .defaultValue("async")
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_ASYNC_COMMIT_BEHAVIOUR = Index.propertyBuilder("sleeper.default.table.statestore.commit.async.behaviour")
            .description("This is the default for whether state store updates will be applied asynchronously via the " +
                    "state store committer.\n" +
                    "This is usually only used for state store implementations where there's a benefit to applying " +
                    "state store updates in a single process for each Sleeper table. This is usually to avoid " +
                    "contention from multiple processes performing updates at the same time.\n" +
                    "This is separate from the properties that determine which state store updates will be done as " +
                    "asynchronous commits. Those properties will only be applied when asynchronous commits are " +
                    "enabled for a given state store.\n" +
                    "Valid values are: " + describeEnumValuesInLowerCase(DefaultAsyncCommitBehaviour.class) + "\n" +
                    "With `disabled`, asynchronous commits will never be used unless overridden in table properties.\n" +
                    "With `per_implementation`, asynchronous commits will be used for all state store implementations " +
                    "that are known to benefit from it, unless overridden in table properties.\n" +
                    "With `all_implementations`, asynchronous commits will be used for all state stores unless " +
                    "overridden in table properties.")
            .defaultValue(DefaultAsyncCommitBehaviour.PER_IMPLEMENTATION.toString())
            .validationPredicate(DefaultAsyncCommitBehaviour::isValid)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_COMPACTION_JOB_ID_ASSIGNMENT_COMMIT_ASYNC = Index.propertyBuilder("sleeper.default.table.compaction.job.id.assignment.commit.async")
            .description("This is the default for whether created compaction jobs will be assigned to their input " +
                    "files asynchronously via the state store committer, if asynchronous commit is enabled. " +
                    "Otherwise, the compaction job creator will commit input file assignments directly to the state " +
                    "store.")
            .defaultValue("true")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_COMPACTION_JOB_COMMIT_ASYNC = Index.propertyBuilder("sleeper.default.table.compaction.job.commit.async")
            .description("This is the default for whether compaction tasks will commit finished jobs asynchronously " +
                    "via the state store committer, if asynchronous commit is enabled. Otherwise, compaction tasks " +
                    "will commit finished jobs directly to the state store.")
            .defaultValue("true")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_COMPACTION_JOB_ASYNC_BATCHING = Index.propertyBuilder("sleeper.default.table.compaction.job.async.commit.batching")
            .description("This property is the default for whether commits of compaction jobs are batched before " +
                    "being sent to the state store commit queue to be applied by the committer lambda. If this property " +
                    "is true and asynchronous commits are enabled then commits of compactions will be batched. If this " +
                    "property is false and asynchronous commits are enabled then commits of compactions will not be " +
                    "batched and will be sent directly to the committer lambda. This property can be overridden for " +
                    "individual tables.")
            .defaultValue("true")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_INGEST_FILES_COMMIT_ASYNC = Index.propertyBuilder("sleeper.default.table.ingest.job.files.commit.async")
            .description("This is the default for whether ingest tasks will add files asynchronously via the state " +
                    "store committer, if asynchronous commit is enabled. Otherwise, ingest tasks will add files " +
                    "directly to the state store.")
            .defaultValue("true")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_FILES_COMMIT_ASYNC = Index.propertyBuilder("sleeper.default.table.bulk.import.job.files.commit.async")
            .description("This is the default for whether bulk import will add files asynchronously via the state " +
                    "store committer, if asynchronous commit is enabled. Otherwise, bulk import will add files " +
                    "directly to the state store.")
            .defaultValue("true")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_PARTITION_SPLIT_ASYNC_COMMIT = Index.propertyBuilder("sleeper.default.table.partition.splitting.commit.async")
            .description("This is the default for whether partition splits will be applied asynchronously via the " +
                    "state store committer, if asynchronous commit is enabled. Otherwise, the partition splitter " +
                    "will apply splits directly to the state store.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_GARBAGE_COLLECTOR_ASYNC_COMMIT = Index.propertyBuilder("sleeper.default.table.gc.commit.async")
            .description("This is the default for whether the garbage collector will record deleted files " +
                    "asynchronously via the state store committer, if asynchronous commit is enabled. Otherwise, the " +
                    "garbage collector will record this directly to the state store.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_STATESTORE_COMMITTER_UPDATE_ON_EVERY_COMMIT = Index.propertyBuilder("sleeper.default.table.statestore.committer.update.every.commit")
            .description("When using the transaction log state store, this sets whether to update from the " +
                    "transaction log before adding a transaction in the asynchronous state store committer.\n" +
                    "If asynchronous commits are used for all or almost all state store updates, this can be false " +
                    "to avoid the extra queries.\n" +
                    "If the state store is commonly updated directly outside of the asynchronous committer, this can " +
                    "be true to avoid conflicts and retries.")
            .defaultValue("false")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_STATESTORE_COMMITTER_UPDATE_ON_EVERY_BATCH = Index.propertyBuilder("sleeper.default.table.statestore.committer.update.every.batch")
            .description("When using the transaction log state store, this sets whether to update from the " +
                    "transaction log before adding a batch of transactions in the asynchronous state store " +
                    "committer.")
            .defaultValue("true")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_DATA_ENGINE = Index.propertyBuilder("sleeper.default.table.data.engine")
            .description("Select which data engine to use for the table. " +
                    "Valid values are: " + describeEnumValuesInLowerCase(DataEngine.class))
            .defaultValue(DataEngine.DATAFUSION.toString())
            .validationPredicate(DataEngine::isValid)
            .propertyGroup(InstancePropertyGroup.TABLE_PROPERTY_DEFAULT).build();

    static List<UserDefinedInstanceProperty> getAll() {
        return Index.INSTANCE.getAll();
    }

    /**
     * An index of property definitions in this file.
     */
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
