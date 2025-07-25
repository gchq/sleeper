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
package sleeper.compaction.datafusion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.compaction.datafusion.DataFusionFunctions.DataFusionCompactionParams;
import sleeper.compaction.datafusion.DataFusionFunctions.DataFusionCompactionResult;
import sleeper.core.partition.Partition;
import sleeper.core.properties.model.CompactionMethod;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.foreign.bridge.FFIBridge;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

import static sleeper.core.properties.table.TableProperty.COLUMN_INDEX_TRUNCATE_LENGTH;
import static sleeper.core.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.core.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS;
import static sleeper.core.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS;
import static sleeper.core.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_VALUE_FIELDS;
import static sleeper.core.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.core.properties.table.TableProperty.PARQUET_WRITER_VERSION;
import static sleeper.core.properties.table.TableProperty.STATISTICS_TRUNCATE_LENGTH;

public class DataFusionCompactionRunner implements CompactionRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataFusionCompactionRunner.class);

    /** Maximum number of rows in a Parquet row group. */
    public static final long DATAFUSION_MAX_ROW_GROUP_ROWS = 1_000_000;

    private final AwsConfig awsConfig;

    private static final DataFusionFunctions NATIVE_COMPACTION;

    static {
        // Obtain native library. This throws an exception if native library can't be
        // loaded and linked
        try {
            NATIVE_COMPACTION = FFIBridge.createForeignInterface(DataFusionFunctions.class);
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public DataFusionCompactionRunner() {
        this(null);
    }

    public DataFusionCompactionRunner(AwsConfig awsConfig) {
        this.awsConfig = awsConfig;
    }

    @Override
    public RowsProcessed compact(CompactionJob job, TableProperties tableProperties, Partition partition) throws IOException {
        jnr.ffi.Runtime runtime = jnr.ffi.Runtime.getRuntime(NATIVE_COMPACTION);

        DataFusionCompactionParams params = createCompactionParams(job, tableProperties, partition.getRegion(), awsConfig, runtime);

        RowsProcessed result = invokeDataFusion(job, params, runtime);

        LOGGER.info("Compaction job {}: compaction finished at {}", job.getId(),
                LocalDateTime.now());
        return result;
    }

    /**
     * Creates the input struct that contains all the information needed by the Rust code
     * side of the compaction.
     *
     * This includes all Parquet writer settings as well as compaction data such as
     * input files, compaction
     * region etc.
     *
     * @param  job             compaction job
     * @param  tableProperties configuration for the Sleeper table
     * @param  region          region being compacted
     * @param  awsConfig       settings to access AWS, or null to use defaults
     * @param  runtime         FFI runtime
     * @return                 object to pass to FFI layer
     */
    @SuppressWarnings(value = "checkstyle:avoidNestedBlocks")
    private static DataFusionCompactionParams createCompactionParams(CompactionJob job, TableProperties tableProperties,
            Region region, AwsConfig awsConfig, jnr.ffi.Runtime runtime) {
        Schema schema = tableProperties.getSchema();
        DataFusionCompactionParams params = new DataFusionCompactionParams(runtime);
        if (awsConfig != null) {
            params.override_aws_config.set(true);
            params.aws_region.set(awsConfig.region);
            params.aws_endpoint.set(awsConfig.endpoint);
            params.aws_allow_http.set(awsConfig.allowHttp);
            params.aws_access_key.set(awsConfig.accessKey);
            params.aws_secret_key.set(awsConfig.secretKey);
        } else {
            params.override_aws_config.set(false);
        }
        params.input_files.populate(job.getInputFiles().toArray(new String[0]), false);
        params.output_file.set(job.getOutputFile());
        params.row_key_cols.populate(schema.getRowKeyFieldNames().toArray(new String[0]), false);
        params.row_key_schema.populate(getKeyTypes(schema.getRowKeyTypes()), false);
        params.sort_key_cols.populate(schema.getSortKeyFieldNames().toArray(new String[0]), false);
        params.max_row_group_size.set(DATAFUSION_MAX_ROW_GROUP_ROWS);
        params.max_page_size.set(tableProperties.getInt(PAGE_SIZE));
        params.compression.set(tableProperties.get(COMPRESSION_CODEC));
        params.writer_version.set(tableProperties.get(PARQUET_WRITER_VERSION));
        params.column_truncate_length.set(tableProperties.getInt(COLUMN_INDEX_TRUNCATE_LENGTH));
        params.stats_truncate_length.set(tableProperties.getInt(STATISTICS_TRUNCATE_LENGTH));
        params.dict_enc_row_keys.set(tableProperties.getBoolean(DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS));
        params.dict_enc_sort_keys.set(tableProperties.getBoolean(DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS));
        params.dict_enc_values.set(tableProperties.getBoolean(DICTIONARY_ENCODING_FOR_VALUE_FIELDS));
        // Is there an aggregation/filtering iterator set?
        if (CompactionMethod.AGGREGATION_ITERATOR_NAME.equals(job.getIteratorClassName())) {
            params.iterator_config.set(job.getIteratorConfig());
        } else {
            params.iterator_config.set("");
        }
        // Extra braces: Make sure wrong array isn't populated to wrong pointers
        {
            // This array can't contain nulls
            Object[] regionMins = region.getRanges().stream().map(Range::getMin).toArray();
            params.region_mins.populate(regionMins, false);
        }
        {
            Boolean[] regionMinInclusives = region.getRanges().stream().map(Range::isMinInclusive)
                    .toArray(Boolean[]::new);
            params.region_mins_inclusive.populate(regionMinInclusives, false);
        }
        {
            // This array can contain nulls
            Object[] regionMaxs = region.getRanges().stream().map(Range::getMax).toArray();
            params.region_maxs.populate(regionMaxs, true);
        }
        {
            Boolean[] regionMaxInclusives = region.getRanges().stream().map(Range::isMaxInclusive)
                    .toArray(Boolean[]::new);
            params.region_maxs_inclusive.populate(regionMaxInclusives, false);
        }
        params.validate();
        return params;
    }

    /**
     * Convert a list of Sleeper primitive types to a number indicating their type
     * for FFI translation.
     *
     * @param  keyTypes              list of primitive types of columns
     * @return                       array of type IDs
     * @throws IllegalStateException if unsupported type found
     */
    public static Integer[] getKeyTypes(List<PrimitiveType> keyTypes) {
        return keyTypes.stream().mapToInt(type -> {
            if (type instanceof IntType) {
                return 1;
            } else if (type instanceof LongType) {
                return 2;
            } else if (type instanceof StringType) {
                return 3;
            } else if (type instanceof ByteArrayType) {
                return 4;
            } else {
                throw new IllegalStateException("Unsupported column type found " + type.getClass());
            }
        }).boxed()
                .toArray(Integer[]::new);
    }

    /**
     * Take the compaction parameters and invoke the DataFusion compactor using the FFI
     * bridge.
     *
     * @param  job              the compaction job
     * @param  compactionParams the compaction input parameters
     * @param  runtime          the JNR FFI runtime object
     * @return                  rows read/written
     * @throws IOException      if the foreign library call doesn't complete successfully
     */
    public static RowsProcessed invokeDataFusion(CompactionJob job,
            DataFusionCompactionParams compactionParams, jnr.ffi.Runtime runtime) throws IOException {
        // Create object to hold the result (in native memory)
        DataFusionCompactionResult compactionData = new DataFusionCompactionResult(runtime);
        // Perform compaction
        int result = NATIVE_COMPACTION.merge_sorted_files(compactionParams, compactionData);

        // Check result
        if (result != 0) {
            LOGGER.error("DataFusion compaction failed, return code: {}", result);
            throw new IOException("DataFusion compaction failed with return code " + result);
        }

        long totalNumberOfRowsRead = compactionData.rows_read.get();
        long rowsWritten = compactionData.rows_written.get();

        LOGGER.info("Compaction job {}: Read {} rows and wrote {} rows",
                job.getId(), totalNumberOfRowsRead, rowsWritten);

        return new RowsProcessed(totalNumberOfRowsRead, rowsWritten);
    }

    @Override
    public String implementationLanguage() {
        return "Rust";
    }

    public static class AwsConfig {
        private final String region;
        private final String endpoint;
        private final String accessKey;
        private final String secretKey;
        private final boolean allowHttp;

        private AwsConfig(Builder builder) {
            region = builder.region;
            endpoint = builder.endpoint;
            accessKey = builder.accessKey;
            secretKey = builder.secretKey;
            allowHttp = builder.allowHttp;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String region;
            private String endpoint;
            private String accessKey;
            private String secretKey;
            private boolean allowHttp;

            private Builder() {
            }

            public Builder region(String region) {
                this.region = region;
                return this;
            }

            public Builder endpoint(String endpoint) {
                this.endpoint = endpoint;
                return this;
            }

            public Builder accessKey(String accessKey) {
                this.accessKey = accessKey;
                return this;
            }

            public Builder secretKey(String secretKey) {
                this.secretKey = secretKey;
                return this;
            }

            public Builder allowHttp(boolean allowHttp) {
                this.allowHttp = allowHttp;
                return this;
            }

            public AwsConfig build() {
                return new AwsConfig(this);
            }
        }
    }
}
