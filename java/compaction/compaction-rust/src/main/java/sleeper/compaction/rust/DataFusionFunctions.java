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
import jnr.ffi.Struct;
import jnr.ffi.annotations.In;
import jnr.ffi.annotations.Out;

import sleeper.foreign.FFIArray;
import sleeper.foreign.ForeignFunctions;

import java.util.Objects;

/**
 * The interface for the native library we are calling.
 */
public interface DataFusionFunctions extends ForeignFunctions {
    /**
     * The compaction output data that the native code will populate.
     */
    @SuppressWarnings(value = {"checkstyle:membername", "checkstyle:parametername"})
    @SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    class DataFusionCompactionResult extends Struct {
        public final Struct.size_t rows_read = new Struct.size_t();
        public final Struct.size_t rows_written = new Struct.size_t();

        public DataFusionCompactionResult(jnr.ffi.Runtime runtime) {
            super(runtime);
        }
    }

    /**
     * The compaction input data that will be populated from the Java side. If you updated
     * this struct (field ordering, types, etc.), you MUST update the corresponding Rust definition
     * in rust/compaction/src/lib.rs. The order and types of the fields must match exactly.
     */
    @SuppressWarnings(value = {"checkstyle:membername"})
    @SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    class DataFusionCompactionParams extends Struct {
        /** Optional AWS configuration. */
        public final Struct.Boolean override_aws_config = new Struct.Boolean();
        public final Struct.UTF8StringRef aws_region = new Struct.UTF8StringRef();
        public final Struct.UTF8StringRef aws_endpoint = new Struct.UTF8StringRef();
        public final Struct.UTF8StringRef aws_access_key = new Struct.UTF8StringRef();
        public final Struct.UTF8StringRef aws_secret_key = new Struct.UTF8StringRef();
        public final Struct.Boolean aws_allow_http = new Struct.Boolean();
        /** Array of input files to compact. */
        public final FFIArray<java.lang.String> input_files = new FFIArray<>(this);
        /** Output file name. */
        public final Struct.UTF8StringRef output_file = new Struct.UTF8StringRef();
        /** Names of Sleeper row key columns from schema. */
        public final FFIArray<java.lang.String> row_key_cols = new FFIArray<>(this);
        /** Types for region schema 1 = Int, 2 = Long, 3 = String, 4 = Byte array. */
        public final FFIArray<java.lang.Integer> row_key_schema = new FFIArray<>(this);
        /** Names of Sleeper sort key columns from schema. */
        public final FFIArray<java.lang.String> sort_key_cols = new FFIArray<>(this);
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
        public final FFIArray<Object> region_mins = new FFIArray<>(this);
        /** Compaction partition region maximums. MUST BE SAME LENGTH AS row_key_cols. */
        public final FFIArray<Object> region_maxs = new FFIArray<>(this);
        /** Compaction partition region minimums are inclusive? MUST BE SAME LENGTH AS row_key_cols. */
        public final FFIArray<java.lang.Boolean> region_mins_inclusive = new FFIArray<>(this);
        /** Compaction partition region maximums are inclusive? MUST BE SAME LENGTH AS row_key_cols. */
        public final FFIArray<java.lang.Boolean> region_maxs_inclusive = new FFIArray<>(this);
        /** Compaction iterator configuration. This is optional. */
        public final Struct.UTF8StringRef iterator_config = new Struct.UTF8StringRef();

        public DataFusionCompactionParams(jnr.ffi.Runtime runtime) {
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
            long rowKeys = row_key_cols.length();
            if (rowKeys != row_key_schema.length()) {
                throw new IllegalStateException("row key schema array has length " + row_key_schema.length() + " but there are " + rowKeys + " row key columns");
            }
            if (rowKeys != region_mins.length()) {
                throw new IllegalStateException("region mins has length " + region_mins.length() + " but there are " + rowKeys + " row key columns");
            }
            if (rowKeys != region_maxs.length()) {
                throw new IllegalStateException("region maxs has length " + region_maxs.length() + " but there are " + rowKeys + " row key columns");
            }
            if (rowKeys != region_mins_inclusive.length()) {
                throw new IllegalStateException("region mins inclusives has length " + region_mins_inclusive.length() + " but there are " + rowKeys + " row key columns");
            }
            if (rowKeys != region_maxs_inclusive.length()) {
                throw new IllegalStateException("region maxs inclusives has length " + region_maxs_inclusive.length() + " but there are " + rowKeys + " row key columns");
            }
        }
    }

    DataFusionCompactionResult allocate_result();

    void free_result(@In DataFusionCompactionResult res);

    @SuppressWarnings(value = "checkstyle:parametername")
    int ffi_merge_sorted_files(@In DataFusionCompactionParams input, @Out DataFusionCompactionResult result);
}
