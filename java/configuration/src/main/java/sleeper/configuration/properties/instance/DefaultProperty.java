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

package sleeper.configuration.properties.instance;

import sleeper.configuration.Utils;
import sleeper.configuration.properties.SleeperPropertyIndex;
import sleeper.configuration.properties.table.CompressionCodec;
import sleeper.configuration.properties.validation.IngestFileWritingStrategy;
import sleeper.configuration.properties.validation.IngestQueue;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;

import java.util.List;
import java.util.Locale;

import static sleeper.configuration.Utils.describeEnumValuesInLowerCase;

public interface DefaultProperty {
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
    UserDefinedInstanceProperty DEFAULT_PARQUET_WRITER_VERSION = Index.propertyBuilder("sleeper.default.parquet.writer.version")
            .description("Used to set parquet.writer.version, see documentation here:\n" +
                    "https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/README.md\n" +
                    "Can be either v1 or v2. The v2 pages store levels uncompressed while v1 pages compress levels with the data.")
            .defaultValue("v2")
            .validationPredicate(List.of("v1", "v2")::contains)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_ADD_TRANSACTION_MAX_ATTEMPTS = Index.propertyBuilder("sleeper.default.metadata.transactionlog.add.transaction.max.attempts")
            .description("The number of attempts to make when applying a transaction to the state store. " +
                    "This default can be overridden by a table property.")
            .defaultValue("" + TransactionLogStateStore.DEFAULT_MAX_ADD_TRANSACTION_ATTEMPTS)
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_ADD_TRANSACTION_FIRST_RETRY_WAIT_CEILING_MS = Index.propertyBuilder("sleeper.default.transactionlog.metadata.add.transaction.first.retry.wait.ceiling.ms")
            .description("The maximum amount of time to wait before the first retry when applying a transaction to " +
                    "the state store. Full jitter will be applied so that the actual wait time will be a random " +
                    "period between 0 and this value. This ceiling will increase exponentially on further retries. " +
                    "See the below article.\n" +
                    "https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/\n" +
                    "This default can be overridden by a table property.")
            .defaultValue("" + TransactionLogStateStore.DEFAULT_RETRY_WAIT_RANGE.getFirstWaitCeiling().toMillis())
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_ADD_TRANSACTION_MAX_RETRY_WAIT_CEILING_MS = Index.propertyBuilder("sleeper.default.transactionlog.metadata.add.transaction.max.retry.wait.ceiling.ms")
            .description("The maximum amount of time to wait before any retry when applying a transaction to " +
                    "the state store. Full jitter will be applied so that the actual wait time will be a random " +
                    "period between 0 and this value. This restricts the exponential increase of the wait ceiling " +
                    "while retrying the transaction. See the below article.\n" +
                    "https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/\n" +
                    "This default can be overridden by a table property.")
            .defaultValue("" + TransactionLogStateStore.DEFAULT_RETRY_WAIT_RANGE.getMaxWaitCeiling().toMillis())
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_TIME_BETWEEN_SNAPSHOT_CHECKS_SECS = Index.propertyBuilder("sleeper.default.metadata.transactionlog.time.between.snapshot.checks.secs")
            .description("The number of seconds to wait after we've loaded a snapshot before looking for a new " +
                    "snapshot. This should relate to the rate at which new snapshots are created, configured in the " +
                    "instance property `sleeper.metadata.transactionlog.snapshot.creation.lambda.period.minutes`. " +
                    "This default can be overridden by a table property.")
            .defaultValue("" + TransactionLogStateStore.DEFAULT_TIME_BETWEEN_SNAPSHOT_CHECKS.toSeconds())
            .validationPredicate(Utils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_TIME_BETWEEN_TRANSACTION_CHECKS_MS = Index.propertyBuilder("sleeper.default.metadata.transactionlog.time.between.transaction.checks.ms")
            .description("The number of milliseconds to wait after we've updated from the transaction log before " +
                    "checking for new transactions. The state visible to an instance of the state store can be out " +
                    "of date by this amount. This can avoid excessive queries by the same process, but can result in " +
                    "unwanted behaviour when using multiple state store objects. When adding a new transaction to " +
                    "update the state, this will be ignored and the state will be brought completely up to date. " +
                    "This default can be overridden by a table property.")
            .defaultValue("" + TransactionLogStateStore.DEFAULT_TIME_BETWEEN_TRANSACTION_CHECKS.toMillis())
            .validationPredicate(Utils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_MIN_TRANSACTIONS_AHEAD_TO_LOAD_SNAPSHOT = Index.propertyBuilder("sleeper.default.metadata.transactionlog.snapshot.load.min.transactions.ahead")
            .description("The minimum number of transactions that a snapshot must be ahead of the local " +
                    "state, before we load the snapshot instead of updating from the transaction log.")
            .defaultValue("" + TransactionLogStateStore.DEFAULT_MIN_TRANSACTIONS_AHEAD_TO_LOAD_SNAPSHOT)
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS = Index.propertyBuilder("sleeper.default.metadata.transactionlog.snapshot.expiry.days")
            .description("The number of days that transaction log snapshots remain in the snapshot store before being deleted.")
            .defaultValue("2")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.DEFAULT)
            .build();
    UserDefinedInstanceProperty DEFAULT_TRANSACTION_LOG_SNAPSHOT_MIN_AGE_MINUTES_TO_DELETE_TRANSACTIONS = Index
            .propertyBuilder("sleeper.default.metadata.transactionlog.delete.behind.snapshot.min.age.minutes")
            .description("The minimum age in minutes of a snapshot in order to allow deletion of transactions " +
                    "leading up to it. When deleting old transactions, there's a chance that processes may still " +
                    "read transactions starting from an older snapshot. We need to avoid deletion of any " +
                    "transactions associated with a snapshot that may still be used as the starting point for " +
                    "reading the log.")
            .defaultValue("2")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.DEFAULT)
            .build();
    UserDefinedInstanceProperty DEFAULT_TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE = Index.propertyBuilder("sleeper.default.metadata.transactionlog.delete.number.behind.latest.snapshot")
            .description("The minimum number of transactions that a transaction must be behind the latest snapshot " +
                    "before being deleted. This is the number of transactions that will be kept and protected from " +
                    "deletion, whenever old transactions are deleted. This includes the transaction that the latest " +
                    "snapshot was created against. Any transactions after the snapshot will never be deleted as they " +
                    "are still in active use.\n" +
                    "This should be configured in relation to the property which determines whether a process will " +
                    "load the latest snapshot or instead seek through the transaction log, since we need to preserve " +
                    "transactions that may still be read:\n" +
                    "sleeper.default.metadata.snapshot.load.min.transactions.ahead\n" +
                    "The snapshot that will be considered the latest snapshot is configured by a property to set the " +
                    "minimum age for it to count for this:\n" +
                    "sleeper.default.metadata.transactionlog.delete.behind.snapshot.min.age\n")
            .defaultValue("200")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.DEFAULT)
            .build();
    UserDefinedInstanceProperty DEFAULT_DYNAMO_STRONGLY_CONSISTENT_READS = Index.propertyBuilder("sleeper.default.table.dynamo.strongly.consistent.reads")
            .description("This specifies whether queries and scans against DynamoDB tables used in the state stores " +
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
    UserDefinedInstanceProperty DEFAULT_INGEST_BATCHER_INGEST_QUEUE = Index.propertyBuilder("sleeper.default.ingest.batcher.ingest.queue")
            .description("Specifies the target ingest queue where batched jobs are sent.\n" +
                    "Valid values are: " + describeEnumValuesInLowerCase(IngestQueue.class))
            .defaultValue(IngestQueue.BULK_IMPORT_EMR_SERVERLESS.name().toLowerCase(Locale.ROOT))
            .validationPredicate(IngestQueue::isValid)
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
    UserDefinedInstanceProperty DEFAULT_INGEST_FILE_WRITING_STRATEGY = Index.propertyBuilder("sleeper.default.ingest.file.writing.strategy")
            .description("Specifies the strategy that ingest uses to create files and references in partitions.\n" +
                    "Valid values are: " + describeEnumValuesInLowerCase(IngestFileWritingStrategy.class))
            .defaultValue(IngestFileWritingStrategy.ONE_REFERENCE_PER_LEAF.name().toLowerCase(Locale.ROOT))
            .validationPredicate(IngestFileWritingStrategy::isValid)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_INGEST_RECORD_BATCH_TYPE = Index.propertyBuilder("sleeper.default.ingest.record.batch.type")
            .description("The way in which records are held in memory before they are written to a local store.\n" +
                    "Valid values are 'arraylist' and 'arrow'.\n" +
                    "The arraylist method is simpler, but it is slower and requires careful tuning of the number of records in each batch.")
            .defaultValue("arrow")
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE = Index.propertyBuilder("sleeper.default.ingest.partition.file.writer.type")
            .description("The way in which partition files are written to the main Sleeper store.\n" +
                    "Valid values are 'direct' (which writes using the s3a Hadoop file system) and 'async' (which writes locally and then " +
                    "copies the completed Parquet file asynchronously into S3).\n" +
                    "The direct method is simpler but the async method should provide better performance when the number of partitions " +
                    "is large.")
            .defaultValue("async")
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_COMPACTION_JOB_COMMIT_ASYNC = Index.propertyBuilder("sleeper.default.compaction.job.commit.async")
            .description("If true, compaction job commit requests will be sent to the state store committer lambda " +
                    "to be performed asynchronously. If false, compaction jobs will be committed synchronously by compaction tasks.")
            .defaultValue("true")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_INGEST_FILES_COMMIT_ASYNC = Index.propertyBuilder("sleeper.default.ingest.job.files.commit.async")
            .description("If true, ingest tasks will add files via requests sent to the state store committer lambda " +
                    "asynchronously. If false, ingest tasks will commit new files synchronously.")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.DEFAULT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_FILES_COMMIT_ASYNC = Index.propertyBuilder("sleeper.default.bulk.import.job.files.commit.async")
            .description("If true, bulk import will add files via requests sent to the state store committer lambda " +
                    "asynchronously. If false, bulk import will commit new files at the end of the job synchronously.")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse)
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

        static UserDefinedInstancePropertyImpl.Builder propertyBuilder(String propertyName) {
            return UserDefinedInstancePropertyImpl.named(propertyName)
                    .addToIndex(INSTANCE::add);
        }
    }
}
