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

import jnr.ffi.Struct;

import java.util.Objects;

import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_COMPRESSION_CODEC;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_DICTIONARY_ENCODING_FOR_VALUE_FIELDS;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_PAGE_SIZE;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_PARQUET_ROWGROUP_ROWS;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_PARQUET_WRITER_VERSION;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_STATISTICS_TRUNCATE_LENGTH;

/**
 * Contains all the Sleeper options for DataFusion operation. These may not be needed for every DataFusion usage,
 * so come with reasonable defaults. All defaults are documented in Rust code.
 *
 * <strong>THIS IS A C COMPATIBLE FFI STRUCT!</strong> If you updated this struct (field ordering, types, etc.),
 * you MUST update the corresponding Rust definition in rust/sleeper_df/src/objects/ffi_sleeper_options.rs. The
 * order and types of the fields must match exactly.
 */
@SuppressWarnings("checkstyle:membername")
public class FFIParquetOptions extends Struct {
    /** Whether Parquet page indexes should be read. */
    public final Struct.Boolean read_page_indexes = new Struct.Boolean();
    /** Maximum size of output Parquet row group in rows. */
    public final Struct.size_t max_row_group_size = new Struct.size_t();
    /** Maximum size of output Parquet page size in bytes. */
    public final Struct.size_t max_page_size = new Struct.size_t();
    /** Output Parquet compression codec. */
    public final Struct.UTF8StringRef compression = new Struct.UTF8StringRef();
    /** Output Parquet writer version. Must be v1 or v2. */
    public final Struct.UTF8StringRef writer_version = new Struct.UTF8StringRef();
    /** Column min/max values truncation length in output Parquet. */
    public final Struct.size_t column_truncate_length = new Struct.size_t();
    /** Max sizeof statistics block in output Parquet. */
    public final Struct.size_t stats_truncate_length = new Struct.size_t();
    /** Should row key fields use dictionary encoding in output Parquet. */
    public final Struct.Boolean dict_enc_row_keys = new Struct.Boolean();
    /** Should sort key fields use dictionary encoding in output Parquet. */
    public final Struct.Boolean dict_enc_sort_keys = new Struct.Boolean();
    /** Should value fields use dictionary encoding in output Parquet. */
    public final Struct.Boolean dict_enc_values = new Struct.Boolean();

    public FFIParquetOptions(jnr.ffi.Runtime runtime) {
        super(runtime);
        read_page_indexes.set(false);
        max_row_group_size.set(java.lang.Long.parseLong(DEFAULT_PARQUET_ROWGROUP_ROWS.getDefaultValue()));
        max_page_size.set(java.lang.Long.parseLong(DEFAULT_PAGE_SIZE.getDefaultValue()));
        compression.set(DEFAULT_COMPRESSION_CODEC.getDefaultValue());
        writer_version.set(DEFAULT_PARQUET_WRITER_VERSION.getDefaultValue());
        column_truncate_length.set(java.lang.Long.parseLong(DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH.getDefaultValue()));
        stats_truncate_length.set(java.lang.Long.parseLong(DEFAULT_STATISTICS_TRUNCATE_LENGTH.getDefaultValue()));
        dict_enc_row_keys.set(java.lang.Boolean.parseBoolean(DEFAULT_DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS.getDefaultValue()));
        dict_enc_sort_keys.set(java.lang.Boolean.parseBoolean(DEFAULT_DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS.getDefaultValue()));
        dict_enc_values.set(java.lang.Boolean.parseBoolean(DEFAULT_DICTIONARY_ENCODING_FOR_VALUE_FIELDS.getDefaultValue()));
    }

    /**
     * Validates the state of this struct.
     *
     * @throws IllegalStateException when a invariant fails
     */
    public void validate() {
        if (max_row_group_size.get() < 1) {
            throw new IllegalStateException("max row group size < 1");
        }
        if (max_page_size.get() < 1) {
            throw new IllegalStateException("max page size < 1");
        }
        if (column_truncate_length.get() < 1) {
            throw new IllegalStateException("column truncate length < 1");
        }
        if (stats_truncate_length.get() < 1) {
            throw new IllegalStateException("stats truncate length < 1");
        }
        Objects.requireNonNull(compression.get(), "Parquet compression codec is null");
        Objects.requireNonNull(writer_version.get(), "Parquet writer is null");
    }
}
