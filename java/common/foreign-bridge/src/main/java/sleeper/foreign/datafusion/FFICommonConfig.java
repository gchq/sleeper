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

import sleeper.foreign.FFIBytes;
import sleeper.foreign.FFISleeperRegion;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * The common DataFusion input data that will be populated from the Java side.
 * <p>
 * <strong>THIS IS A C COMPATIBLE FFI STRUCT!</strong> If you updated this struct (field ordering, types, etc.),
 * you MUST update the corresponding Rust definition in rust/sleeper_df/src/objects/ffi_common_config.rs. The order and
 * types of the fields must match exactly.
 */
@SuppressWarnings("checkstyle:membername")
@SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD", "PA_PUBLIC_MUTABLE_OBJECT_ATTRIBUTE"})
public class FFICommonConfig extends Struct {
    /** Job ID. */
    public final Struct.UTF8StringRef job_id = new Struct.UTF8StringRef();
    /** Optional AWS configuration. Set to NULL if not used. */
    public final Struct.StructRef<FFIAwsConfig> aws_config = new Struct.StructRef<>(FFIAwsConfig.class);
    /** Prevent GC. */
    private FFIAwsConfig javaAwsConfig;
    /** Length of input files array. */
    public final Struct.size_t input_files_len = new Struct.size_t();
    /** Array of input files to compact. */
    public final Struct.StructRef<FFIBytes> input_files = new Struct.StructRef<>(FFIBytes.class);
    /** Prevent GC. */
    private FFIBytes[] javaInputFiles;
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
    public final Struct.StructRef<FFIBytes> row_key_cols = new Struct.StructRef<>(FFIBytes.class);
    /** Prevent GC. */
    private FFIBytes[] javaRowKeyCols;
    /** Length of sort keys array. */
    public final Struct.size_t sort_keys_cols_len = new Struct.size_t();
    /** Names of Sleeper sort key fields from schema. */
    public final Struct.StructRef<FFIBytes> sort_key_cols = new Struct.StructRef<>(FFIBytes.class);
    /** Prevent GC. */
    private FFIBytes[] javaSortKeyCols;
    /** The Sleeper compaction region. */
    public final Struct.StructRef<FFISleeperRegion> region = new StructRef<>(FFISleeperRegion.class);
    /** Prevent GC. */
    private FFISleeperRegion javaRegion;
    /** Compaction aggregation configuration. This is optional. */
    public final Struct.UTF8StringRef aggregation_config = new Struct.UTF8StringRef();
    /** Compaction filtering configuration. This is optional. */
    public final Struct.UTF8StringRef filtering_config = new Struct.UTF8StringRef();
    /** Parquet options for Sleeper. Set to NULL if defaults are suitable. */
    public final Struct.StructRef<FFIParquetOptions> parquet_options = new Struct.StructRef<>(FFIParquetOptions.class);
    /** Prevent GC. */
    private FFIParquetOptions javaParquetOptions;

    public FFICommonConfig(jnr.ffi.Runtime runtime) {
        this(runtime, null);
    }

    public FFICommonConfig(jnr.ffi.Runtime runtime, DataFusionAwsConfig awsConfig) {
        super(runtime);
        setAwsConfig(awsConfig);
        // Set to sensible defaults all members that don't have them.
        // Primitives will all default to false/zero.
        output_file.set("");
        aggregation_config.set("");
        filtering_config.set("");
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

    /**
     * Set the DataFusion AWS configuration.
     *
     * @param awsConfig AWS configuration
     */
    public void setAwsConfig(DataFusionAwsConfig awsConfig) {
        if (awsConfig != null) {
            FFIAwsConfig ffiAwsConfig = awsConfig.toFfi(getRuntime());
            this.aws_config.set(ffiAwsConfig);
            this.javaAwsConfig = ffiAwsConfig;
        } else {
            this.aws_config.set(0);
            this.javaAwsConfig = null;
        }
    }

    /**
     * Set the Sleeper partition region.
     *
     * @param region partition region
     */
    public void setRegion(FFISleeperRegion region) {
        this.region.set(region);
        this.javaRegion = region;
    }

    /**
     * Set the Parquet reading and writing options.
     *
     * @param parquetOptions Parquet options
     */
    public void setParquetOptions(FFIParquetOptions parquetOptions) {
        this.parquet_options.set(parquetOptions);
        this.javaParquetOptions = parquetOptions;
    }

    /**
     * Set the list of input file names.
     *
     * @param files input file array
     */
    public void setInputFiles(java.lang.String[] files) {
        input_files_len.set(files.length);
        javaInputFiles = toFFIBytes(files, "files");
        input_files.set(javaInputFiles);
    }

    /**
     * Set row key column names in FFI struct.
     *
     * @param rowKeyCols array of row key names
     */
    public void setRowKeyCols(java.lang.String[] rowKeyCols) {
        row_key_cols_len.set(rowKeyCols.length);
        javaRowKeyCols = toFFIBytes(rowKeyCols, "rowKeyCols");
        row_key_cols.set(javaRowKeyCols);
    }

    /**
     * Set sort key column names in FFI struct.
     *
     * @param sortKeyCols array of row key names
     */
    public void setSortKeyCols(java.lang.String[] sortKeyCols) {
        sort_keys_cols_len.set(sortKeyCols.length);
        javaSortKeyCols = toFFIBytes(sortKeyCols, "sortKeyCols");
        sort_key_cols.set(javaSortKeyCols);
    }

    private FFIBytes[] toFFIBytes(java.lang.String[] values, java.lang.String name) {
        FFIBytes[] result = new FFIBytes[values.length];
        for (int i = 0; i < values.length; i++) {
            Objects.requireNonNull(values[i], "%s[%d]".formatted(name, i));
            result[i] = new FFIBytes(getRuntime(), values[i].getBytes(StandardCharsets.UTF_8));
        }
        return result;
    }
}
