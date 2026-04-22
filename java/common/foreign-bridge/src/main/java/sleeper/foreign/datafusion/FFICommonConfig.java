/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.foreign.datafusion;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import jnr.ffi.Struct;

import sleeper.foreign.FFISleeperRegion;

import java.util.Objects;

/**
 * The common DataFusion input data that will be populated from the Java side.
 *
 * <strong>THIS IS A C COMPATIBLE FFI STRUCT!</strong> If you updated this struct (field ordering, types, etc.),
 * you MUST update the corresponding Rust definition in rust/sleeper_df/src/objects/ffi_common_config.rs. The order and
 * types of the fields must match exactly.
 */
@SuppressWarnings("checkstyle:membername")
@SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public class FFICommonConfig extends Struct {
    /** Optional AWS configuration. Set to NULL if not used. */
    public final Struct.StructRef<FFIAwsConfig> aws_config = new Struct.StructRef<>(FFIAwsConfig.class);
    /** Length of input files array. */
    public final Struct.size_t input_files_len = new Struct.size_t();
    /** Array of input files to compact. */
    public final Struct.Pointer input_files = new Struct.Pointer();
    /** Prevent GC. */
    private jnr.ffi.Pointer java_input_files;
    /** Whether the input files are individually sorted by the row and sort key fields. */
    public final Struct.Boolean input_files_sorted = new Struct.Boolean();
    /** Output file name. */
    public final Struct.UTF8StringRef output_file = new Struct.UTF8StringRef();
    /** Specifies if sketch output is enabled. Can only be used with file output. */
    public final Struct.Boolean write_sketch_file = new Struct.Boolean();
    /** Whether we should use readahead when reading from S3. */
    public final Struct.Boolean use_readahead_store = new Struct.Boolean();
    /** Length of row keys array. */
    public final Struct.size_t row_key_cols_len = new Struct.size_t();
    /** Names of Sleeper row key fields from schema. */
    public final Struct.Pointer row_key_cols = new Struct.Pointer();
    /** Prevent GC. */
    private jnr.ffi.Pointer java_row_key_cols;
    /** Length of sort keys array. */
    public final Struct.size_t sort_keys_cols_len = new Struct.size_t();
    /** Names of Sleeper sort key fields from schema. */
    public final Struct.Pointer sort_key_cols = new Struct.Pointer();
    /** Prevent GC. */
    private jnr.ffi.Pointer java_sort_key_cols;
    /** The Sleeper compaction region. */
    public final Struct.StructRef<FFISleeperRegion> region = new StructRef<>(FFISleeperRegion.class);
    /** Compaction aggregation configuration. This is optional. */
    public final Struct.UTF8StringRef aggregation_config = new Struct.UTF8StringRef();
    /** Compaction filtering configuration. This is optional. */
    public final Struct.UTF8StringRef filtering_config = new Struct.UTF8StringRef();
    /** Parquet options for Sleeper. Set to NULL if defaults are suitable. */
    public final Struct.StructRef<FFIParquetOptions> parquet_options = new Struct.StructRef<>(FFIParquetOptions.class);

    public FFICommonConfig(jnr.ffi.Runtime runtime) {
        this(runtime, null);
    }

    public FFICommonConfig(jnr.ffi.Runtime runtime, DataFusionAwsConfig awsConfig) {
        super(runtime);
        if (awsConfig != null) {
            aws_config.set(awsConfig.toFfi(runtime));
        } else {
            // Null will use default AWS credentials
            aws_config.set(0);
        }
        // Set to sensible defaults all members that don't have them.
        // Primitives will all default to false/zero, FFIArrays also have safe defaults.
        output_file.set("");
        // Null here tells Rust to use defaults.
        parquet_options.set(0);
    }

    /**
     * Validates the state of this struct.
     *
     * @throws IllegalStateException when a invariant fails
     */
    public void validate() {
        // Check strings non null
        Objects.requireNonNull(output_file.get(), "Output file is null");
        Objects.requireNonNull(aggregation_config.get(), "Aggregation configuration is null");
        Objects.requireNonNull(filtering_config.get(), "Filtering configuration is null");
    }
}
