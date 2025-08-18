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
package sleeper.foreign.datafusion;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import jnr.ffi.Struct;

import sleeper.foreign.FFISleeperRegion;
import sleeper.foreign.bridge.FFIArray;

import java.util.Objects;

/**
 * The compaction input data that will be populated from the Java side. If you updated
 * this struct (field ordering, types, etc.), you MUST update the corresponding Rust definition
 * in rust/sleeper_df/src/objects.rs. The order and types of the fields must match exactly.
 */
@SuppressWarnings(value = {"checkstyle:membername"})
@SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class FFIDataFusionCommonConfig extends Struct {
    /** Optional AWS configuration. */
    public final Struct.Boolean override_aws_config = new Struct.Boolean();
    public final Struct.UTF8StringRef aws_region = new Struct.UTF8StringRef();
    public final Struct.UTF8StringRef aws_endpoint = new Struct.UTF8StringRef();
    public final Struct.UTF8StringRef aws_access_key = new Struct.UTF8StringRef();
    public final Struct.UTF8StringRef aws_secret_key = new Struct.UTF8StringRef();
    public final Struct.Boolean aws_allow_http = new Struct.Boolean();
    /** Array of input files to compact. */
    public final FFIArray<java.lang.String> input_files = new FFIArray<>(this);
    /** States if the input files individually sorted based on row key and the sort key columns. */
    public final Struct.Boolean input_files_sorted = new Struct.Boolean();
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
    /** The Sleeper compaction region. */
    public final Struct.StructRef<FFISleeperRegion> region = new StructRef<>(FFISleeperRegion.class);
    /** Strong reference to prevent GC. */
    public FFISleeperRegion regionRef;
    /** Compaction iterator configuration. This is optional. */
    public final Struct.UTF8StringRef iterator_config = new Struct.UTF8StringRef();

    public FFIDataFusionCommonConfig(jnr.ffi.Runtime runtime) {
        super(runtime);
    }

    /**
     * Set the Sleeper partition region.
     *
     * @param newRegion region to transfer across to foreign function
     */
    public void setRegion(FFISleeperRegion newRegion) {
        this.regionRef = newRegion;
        this.region.set(newRegion);
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
        if (regionRef != null) {
            regionRef.validate();
        }
        if (row_key_cols.length() != row_key_schema.length()) {
            throw new IllegalStateException("row_key_schema has length " + row_key_schema.length() + " but there are " + row_key_cols.length() + " row key columns");
        }
        // Check strings non null
        Objects.requireNonNull(output_file.get(), "Output file is null");
        Objects.requireNonNull(writer_version.get(), "Parquet writer is null");
        Objects.requireNonNull(compression.get(), "Parquet compression codec is null");
        Objects.requireNonNull(iterator_config.get(), "Iterator configuration is null");
    }
}
