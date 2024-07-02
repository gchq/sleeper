/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.compaction.job.execution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionRunner;
import sleeper.compaction.job.execution.RustBridge.FFICompactionParams;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import static sleeper.configuration.properties.table.TableProperty.COLUMN_INDEX_TRUNCATE_LENGTH;
import static sleeper.configuration.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.configuration.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS;
import static sleeper.configuration.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS;
import static sleeper.configuration.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_VALUE_FIELDS;
import static sleeper.configuration.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.configuration.properties.table.TableProperty.PARQUET_WRITER_VERSION;
import static sleeper.configuration.properties.table.TableProperty.STATISTICS_TRUNCATE_LENGTH;

public class RustCompaction implements CompactionRunner {
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;

    /** Maximum number of rows in a Parquet row group. */
    private static final long RUST_MAX_ROW_GROUP_ROWS = 1_000_000;

    private static final Logger LOGGER = LoggerFactory.getLogger(RustCompaction.class);

    public RustCompaction(TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider) {
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
    }

    @Override
    public RecordsProcessed compact(CompactionJob job) throws Exception {
        TableProperties tableProperties = tablePropertiesProvider
                .getById(job.getTableId());
        Schema schema = tableProperties.getSchema();
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        Region region = stateStore.getAllPartitions().stream()
                .filter(p -> Objects.equals(job.getPartitionId(), p.getId()))
                .findFirst().orElseThrow(() -> new NoSuchElementException("Partition not found for compaction job"))
                .getRegion();

        // Obtain native library. This throws an exception if native library can't be
        // loaded and linked
        RustBridge.Compaction nativeLib = RustBridge.getRustCompactor();
        jnr.ffi.Runtime runtime = jnr.ffi.Runtime.getRuntime(nativeLib);

        FFICompactionParams params = createFFIParams(job, tableProperties, schema, region, runtime);

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
     * @param  tableProperties table configuration for this table
     * @param  schema          the table schema
     * @param  region          region being compacted
     * @param  runtime         FFI runtime
     * @return                 object to pass to FFI layer
     */
    @SuppressWarnings(value = "checkstyle:avoidNestedBlocks")
    public static FFICompactionParams createFFIParams(CompactionJob job, TableProperties tableProperties, Schema schema,
            Region region, jnr.ffi.Runtime runtime) {
        FFICompactionParams params = new FFICompactionParams(runtime);
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
            LOGGER.info("Invoking native Rust compaction...");

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

    @Override
    public boolean isHardwareAccelerated() {
        return false;
    }

    @Override
    public boolean supportsIterators() {
        return false;
    }
}
