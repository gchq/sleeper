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

import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.foreign.FFISleeperRegion;
import sleeper.foreign.bridge.FFIArray;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The common DataFusion input data that will be populated from the Java side.
 *
 * <strong>THIS IS A C COMPATIBLE FFI STRUCT!</strong> If you updated this struct (field ordering, types, etc.),
 * you MUST update the corresponding Rust definition in rust/sleeper_df/src/objects.rs. The order and types of
 * the fields must match exactly.
 */
@SuppressWarnings(value = {"checkstyle:membername"})
@SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class FFICommonConfig extends Struct {
    /** Specifies AWS default configuration been overriden. */
    public final Struct.Boolean override_aws_config = new Struct.Boolean();
    /** Optional AWS configuration. */
    public final Struct.StructRef<FFIAwsConfig> aws_config = new Struct.StructRef<>(FFIAwsConfig.class);
    /** Array of input files to compact. */
    public final FFIArray<java.lang.String> input_files = new FFIArray<>(this);
    /** States if the input files individually sorted based on row key and the sort key columns. */
    public final Struct.Boolean input_files_sorted = new Struct.Boolean();
    /** Output file name. */
    public final Struct.UTF8StringRef output_file = new Struct.UTF8StringRef();
    /** Specifies if sketch output is enabled. Can only be used with file output. */
    public final Struct.Boolean write_sketch_file = new Struct.Boolean();
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
    /** Compaction iterator configuration. This is optional. */
    public final Struct.UTF8StringRef iterator_config = new Struct.UTF8StringRef();

    public FFICommonConfig(jnr.ffi.Runtime runtime) {
        this(runtime, Optional.empty());
    }

    public FFICommonConfig(jnr.ffi.Runtime runtime, Optional<FFIAwsConfig> awsConfig) {
        super(runtime);
        this.setAWSCredentials(runtime, awsConfig);
        setDefaults();
    }

    /** Set to sensible defaults all members that don't have them. */
    protected void setDefaults() {
        // Primitives will all default to false/zero, FFIArrays also have safe defaults.
        output_file.set("");
        compression.set("");
        writer_version.set("");
        iterator_config.set("");
    }

    /**
     * Set the Sleeper partition region.
     *
     * @param newRegion region to transfer across to foreign function
     */
    public void setRegion(FFISleeperRegion newRegion) {
        newRegion.validate();
        this.region.set(newRegion);
    }

    /**
     * Configure the AWS credentials.
     *
     * @param runtime the JNR runtime
     * @param config  the optional AWS credentials
     */
    public void setAWSCredentials(jnr.ffi.Runtime runtime, Optional<FFIAwsConfig> config) {
        config.ifPresentOrElse(awsConfig -> {
            this.override_aws_config.set(true);
            this.aws_config.set(awsConfig);
        }, () -> {
            this.override_aws_config.set(false);
        });
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
        if (row_key_cols.length() != row_key_schema.length()) {
            throw new IllegalStateException("row_key_schema has length " + row_key_schema.length() + " but there are " + row_key_cols.length() + " row key columns");
        }
        // Check strings non null
        Objects.requireNonNull(output_file.get(), "Output file is null");
        Objects.requireNonNull(writer_version.get(), "Parquet writer is null");
        Objects.requireNonNull(compression.get(), "Parquet compression codec is null");
        Objects.requireNonNull(iterator_config.get(), "Iterator configuration is null");
    }

    /**
     * Convert a list of Sleeper primitive types to an ordinal indicating their type
     * for FFI translation.
     *
     * @param  keyTypes              list of primitive types of columns
     * @return                       array of type IDs
     * @throws IllegalStateException if unsupported type found
     */
    public static Integer[] getKeyTypes(List<PrimitiveType> keyTypes) {
        /*
         * IMPORTANT: These must match the ordinals defined in rust/sleeper_df/src/objects.rs
         */
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
}
