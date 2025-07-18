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
package sleeper.compaction.rust;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionRunner;
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
import sleeper.core.tracker.job.run.RecordsProcessed;

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

public class RustCompactionRunner implements CompactionRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(RustCompactionRunner.class);

    /** Maximum number of rows in a Parquet row group. */
    public static final long RUST_MAX_ROW_GROUP_ROWS = 1_000_000;

    private final AwsConfig awsConfig;

    private static Compaction nativeCompaction = null;

    public RustCompactionRunner() {
        this(null);
    }

    public RustCompactionRunner(AwsConfig awsConfig) {
        this.awsConfig = awsConfig;
    }

    /**
     * The compaction output data that the native code will populate.
     */
    @SuppressWarnings(value = {"checkstyle:membername", "checkstyle:parametername"})
    @SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class FFICompactionResult extends Struct {
        public final Struct.size_t rows_read = new Struct.size_t();
        public final Struct.size_t rows_written = new Struct.size_t();

        public FFICompactionResult(jnr.ffi.Runtime runtime) {
            super(runtime);
        }
    }

    /**
     * The interface for the native library we are calling.
     */
    public interface Compaction extends ForeignFunctions {
        FFICompactionResult allocate_result();

        void free_result(@In FFICompactionResult res);

        @SuppressWarnings(value = "checkstyle:parametername")
        int ffi_merge_sorted_files(@In FFICompactionParams input, @Out FFICompactionResult result);
    }

    @Override
    public RecordsProcessed compact(CompactionJob job, TableProperties tableProperties, Partition partition) throws IOException {

        // Obtain native library. This throws an exception if native library can't be
        // loaded and linked
        RustBridge.Compaction nativeLib = RustBridge.getRustCompactor();
        jnr.ffi.Runtime runtime = jnr.ffi.Runtime.getRuntime(nativeLib);

        FFICompactionParams params = createFFIParams(job, tableProperties, partition.getRegion(), awsConfig, runtime);

        RecordsProcessed result = invokeRustFFI(job, nativeLib, params);

        LOGGER.info("Compaction job {}: compaction finished at {}", job.getId(),
                LocalDateTime.now());
        return result;
    }

    /**
     * Creates the input struct that contains all the information needed by the Rust
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
    private static FFICompactionParams createFFIParams(CompactionJob job, TableProperties tableProperties,
            Region region, AwsConfig awsConfig, jnr.ffi.Runtime runtime) {
        Schema schema = tableProperties.getSchema();
        FFICompactionParams params = new FFICompactionParams(runtime);
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
        params.max_row_group_size.set(RUST_MAX_ROW_GROUP_ROWS);
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
     * The compaction input data that will be populated from the Java side. If you updated
     * this struct (field ordering, types, etc.), you MUST update the corresponding Rust definition
     * in rust/compaction/src/lib.rs. The order and types of the fields must match exactly.
     */
    @SuppressWarnings(value = {"checkstyle:membername"})
    @SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class FFICompactionParams extends Struct {
        /** Optional AWS configuration. */
        public final Struct.Boolean override_aws_config = new Struct.Boolean();
        public final Struct.UTF8StringRef aws_region = new Struct.UTF8StringRef();
        public final Struct.UTF8StringRef aws_endpoint = new Struct.UTF8StringRef();
        public final Struct.UTF8StringRef aws_access_key = new Struct.UTF8StringRef();
        public final Struct.UTF8StringRef aws_secret_key = new Struct.UTF8StringRef();
        public final Struct.Boolean aws_allow_http = new Struct.Boolean();
        /** Array of input files to compact. */
        public final Array<java.lang.String> input_files = new Array<>(this);
        /** Output file name. */
        public final Struct.UTF8StringRef output_file = new Struct.UTF8StringRef();
        /** Names of Sleeper row key columns from schema. */
        public final Array<java.lang.String> row_key_cols = new Array<>(this);
        /** Types for region schema 1 = Int, 2 = Long, 3 = String, 4 = Byte array. */
        public final Array<java.lang.Integer> row_key_schema = new Array<>(this);
        /** Names of Sleeper sort key columns from schema. */
        public final Array<java.lang.String> sort_key_cols = new Array<>(this);
        /** Maximum size of output Parquet row group in rows. */
        public final Struct.size_t max_row_group_size = new Struct.size_t();
        /** Maximum size of output Parquet page size in bytes. */
        public final Struct.size_t max_page_size = new Struct.size_t();
        /** Output Parquet compression codec. */
        public final Struct.UTF8StringRef compression = new Struct.UTF8StringRef();
        /** Output Parquet writer version. Must be 1.0 or 2.0 */
        public final Struct.UTF8StringRef writer_version = new Struct.UTF8StringRef();
        /** Column min/max values truncation length in output Parquet. */
        public final Struct.size_t column_truncate_length = new Struct.size_t();
        /** Max sizeof statistics block in output Parquet. */
        public final Struct.size_t stats_truncate_length = new Struct.size_t();
        /** Should row key columns use dictionary encoding in output Parquet. */
        public final Struct.Boolean dict_enc_row_keys = new Struct.Boolean();
        /** Should sort key columns use dictionary encoding in output Parquet. */
        public final Struct.Boolean dict_enc_sort_keys = new Struct.Boolean();
        /** Should value columns use dictionary encoding in output Parquet. */
        public final Struct.Boolean dict_enc_values = new Struct.Boolean();
        /** Compaction partition region minimums. MUST BE SAME LENGTH AS row_key_cols. */
        public final Array<Object> region_mins = new Array<>(this);
        /** Compaction partition region maximums. MUST BE SAME LENGTH AS row_key_cols. */
        public final Array<Object> region_maxs = new Array<>(this);
        /** Compaction partition region minimums are inclusive? MUST BE SAME LENGTH AS row_key_cols. */
        public final Array<java.lang.Boolean> region_mins_inclusive = new Array<>(this);
        /** Compaction partition region maximums are inclusive? MUST BE SAME LENGTH AS row_key_cols. */
        public final Array<java.lang.Boolean> region_maxs_inclusive = new Array<>(this);
        /** Compaction iterator configuration. This is optional. */
        public final Struct.UTF8StringRef iterator_config = new Struct.UTF8StringRef();

        public FFICompactionParams(jnr.ffi.Runtime runtime) {
            super(runtime);
        }

        /**
         * Validate state of struct.
         *
         * @throws IllegalStateException when a invariant fails
         */
        public void validate() {
            input_files.validate();
            row_key_cols.validate();
            row_key_schema.validate();
            sort_key_cols.validate();
            region_mins.validate();
            region_maxs.validate();
            region_mins_inclusive.validate();
            region_maxs_inclusive.validate();

            // Check strings non null
            Objects.requireNonNull(output_file.get(), "Output file is null");
            Objects.requireNonNull(writer_version.get(), "Parquet writer is null");
            Objects.requireNonNull(compression.get(), "Parquet compression codec is null");
            Objects.requireNonNull(iterator_config.get(), "Iterator configuration is null");

            // Check lengths
            long rowKeys = row_key_cols.len.get();
            if (rowKeys != row_key_schema.len.get()) {
                throw new IllegalStateException("row key schema array has length " + row_key_schema.len.get() + " but there are " + rowKeys + " row key columns");
            }
            if (rowKeys != region_mins.len.get()) {
                throw new IllegalStateException("region mins has length " + region_mins.len.get() + " but there are " + rowKeys + " row key columns");
            }
            if (rowKeys != region_maxs.len.get()) {
                throw new IllegalStateException("region maxs has length " + region_maxs.len.get() + " but there are " + rowKeys + " row key columns");
            }
            if (rowKeys != region_mins_inclusive.len.get()) {
                throw new IllegalStateException("region mins inclusives has length " + region_mins_inclusive.len.get() + " but there are " + rowKeys + " row key columns");
            }
            if (rowKeys != region_maxs_inclusive.len.get()) {
                throw new IllegalStateException("region maxs inclusives has length " + region_maxs_inclusive.len.get() + " but there are " + rowKeys + " row key columns");
            }
        }
    }

    /**
     * Take the compaction parameters and invoke the Rust compactor using the FFI
     * bridge.
     *
     * @param  job              the compaction job
     * @param  nativeLib        the native library implement the FFI bridge
     * @param  compactionParams the compaction input parameters
     * @return                  records read/written
     * @throws IOException      if the Rust library doesn't complete successfully
     */
    public static RecordsProcessed invokeRustFFI(CompactionJob job, RustBridge.Compaction nativeLib,
            FFICompactionParams compactionParams) throws IOException {
        // Create object to hold the result (in native memory)
        RustBridge.FFICompactionResult compactionData = nativeLib.allocate_result();
        try {
            // Perform compaction
            int result = nativeLib.ffi_merge_sorted_files(compactionParams, compactionData);

            // Check result
            if (result != 0) {
                LOGGER.error("Rust compaction failed, return code: {}", result);
                throw new IOException("Rust compaction failed with return code " + result);
            }

            long totalNumberOfRecordsRead = compactionData.rows_read.get();
            long recordsWritten = compactionData.rows_written.get();

            LOGGER.info("Compaction job {}: Read {} records and wrote {} records",
                    job.getId(), totalNumberOfRecordsRead, recordsWritten);

            return new RecordsProcessed(totalNumberOfRecordsRead, recordsWritten);
        } finally {
            // Ensure de-allocation
            nativeLib.free_result(compactionData);
        }
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
