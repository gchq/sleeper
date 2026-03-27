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
    /** Array of input files to compact. */
    public final FFIArray<java.lang.String> input_files = new FFIArray<>(this);
    /** Whether the input files are individually sorted by the row and sort key fields. */
    public final Struct.Boolean input_files_sorted = new Struct.Boolean();
    /** Output file name. */
    public final Struct.UTF8StringRef output_file = new Struct.UTF8StringRef();
    /** Specifies if sketch output is enabled. Can only be used with file output. */
    public final Struct.Boolean write_sketch_file = new Struct.Boolean();
    /** Names of Sleeper row key fields from schema. */
    public final FFIArray<java.lang.String> row_key_cols = new FFIArray<>(this);
    /** Types for region schema 1 = Int, 2 = Long, 3 = String, 4 = Byte array. */
    public final FFIArray<java.lang.Integer> row_key_schema = new FFIArray<>(this);
    /** Names of Sleeper sort key fields from schema. */
    public final FFIArray<java.lang.String> sort_key_cols = new FFIArray<>(this);
    /** The Sleeper compaction region. */
    public final Struct.StructRef<FFISleeperRegion> region = new StructRef<>(FFISleeperRegion.class);
    /** Compaction aggregation configuration. This is optional. */
    public final Struct.UTF8StringRef aggregation_config = new Struct.UTF8StringRef();
    /** Compaction filtering configuration. This is optional. */
    public final Struct.UTF8StringRef filtering_config = new Struct.UTF8StringRef();
    /** Sleeper options. */
    public final Struct.StructRef<FFISleeperOptions> sleeper_options = new Struct.StructRef<>(FFISleeperOptions.class);

    public FFICommonConfig(jnr.ffi.Runtime runtime) {
        this(runtime, null);
    }

    public FFICommonConfig(jnr.ffi.Runtime runtime, DataFusionAwsConfig awsConfig) {
        super(runtime);
        if (awsConfig != null) {
            this.aws_config.set(awsConfig.toFfi(runtime));
        } else {
            this.aws_config.set((FFIAwsConfig) null);
        }
        // Set to sensible defaults all members that don't have them.
        // Primitives will all default to false/zero, FFIArrays also have safe defaults.
        output_file.set("");
        compression.set("");
        writer_version.set("");
    }

    /**
     * Validates the state of this struct.
     *
     * @throws IllegalStateException when a invariant fails
     */
    public void validate() {
        input_files.validate();
        row_key_cols.validate();
        row_key_schema.validate();
        sort_key_cols.validate();
        if (row_key_cols.length() != row_key_schema.length()) {
            throw new IllegalStateException("row_key_schema has length " + row_key_schema.length() + " but there are " + row_key_cols.length() + " row key fields");
        }
        // Check strings non null
        Objects.requireNonNull(output_file.get(), "Output file is null");
        Objects.requireNonNull(writer_version.get(), "Parquet writer is null");
        Objects.requireNonNull(compression.get(), "Parquet compression codec is null");
        Objects.requireNonNull(aggregation_config.get(), "Aggregation configuration is null");
        Objects.requireNonNull(filtering_config.get(), "Filtering configuration is null");
    }

    /**
     * Converts a list of Sleeper primitive types to an ordinal indicating their type
     * for FFI translation.
     *
     * @param  keyTypes              list of primitive types of fields
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
                throw new IllegalStateException("Unsupported field type found " + type.getClass());
            }
        }).boxed().toArray(Integer[]::new);
    }
}
