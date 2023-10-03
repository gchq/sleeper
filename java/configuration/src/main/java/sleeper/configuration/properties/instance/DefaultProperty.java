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

package sleeper.configuration.properties.instance;


import sleeper.configuration.Utils;
import sleeper.configuration.properties.SleeperPropertyIndex;
import sleeper.configuration.properties.table.CompressionCodec;
import sleeper.configuration.properties.validation.BatchIngestMode;

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
    UserDefinedInstanceProperty DEFAULT_DYNAMO_STRONGLY_CONSISTENT_READS = Index.propertyBuilder("sleeper.default.table.dynamo.strongly.consistent.reads")
            .description("This specifies whether queries and scans against DynamoDB tables used in the DynamoDB state store " +
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
    UserDefinedInstanceProperty DEFAULT_INGEST_BATCHER_INGEST_MODE = Index.propertyBuilder("sleeper.default.ingest.batcher.ingest.mode")
            .description("Specifies the target ingest queue where batched jobs are sent.\n" +
                    "Valid values are: " + describeEnumValuesInLowerCase(BatchIngestMode.class))
            .defaultValue(BatchIngestMode.STANDARD_INGEST.name().toLowerCase(Locale.ROOT))
            .validationPredicate(BatchIngestMode::isValidMode)
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
