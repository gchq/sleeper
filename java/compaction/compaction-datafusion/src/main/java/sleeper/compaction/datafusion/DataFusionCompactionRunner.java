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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Region;
import sleeper.core.schema.Schema;
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.foreign.FFISleeperRegion;
import sleeper.foreign.bridge.FFIBridge;
import sleeper.foreign.bridge.FFIContext;
import sleeper.foreign.datafusion.DataFusionAwsConfig;
import sleeper.foreign.datafusion.FFICommonConfig;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Optional;

import static sleeper.core.properties.table.TableProperty.COLUMN_INDEX_TRUNCATE_LENGTH;
import static sleeper.core.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.core.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS;
import static sleeper.core.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS;
import static sleeper.core.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_VALUE_FIELDS;
import static sleeper.core.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.core.properties.table.TableProperty.PARQUET_WRITER_VERSION;
import static sleeper.core.properties.table.TableProperty.STATISTICS_TRUNCATE_LENGTH;

@SuppressFBWarnings("UUF_UNUSED_FIELD")
public class DataFusionCompactionRunner implements CompactionRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataFusionCompactionRunner.class);
    public static final long DATAFUSION_MAX_ROW_GROUP_ROWS = 1_000_000;

    /** Maximum number of rows in a Parquet row group. */
    private static final DataFusionCompactionFunctions NATIVE_COMPACTION;

    private final DataFusionAwsConfig awsConfig;

    static {
        // Obtain native library. This throws an exception if native library can't be
        // loaded and linked
        try {
            NATIVE_COMPACTION = FFIBridge.createForeignInterface(DataFusionCompactionFunctions.class);
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public DataFusionCompactionRunner() {
        this(DataFusionAwsConfig.getDefault());
    }

    public DataFusionCompactionRunner(DataFusionAwsConfig awsConfig) {
        this.awsConfig = awsConfig;
    }

    @Override
    public RowsProcessed compact(CompactionJob job, TableProperties tableProperties, Region region) throws IOException {
        jnr.ffi.Runtime runtime = jnr.ffi.Runtime.getRuntime(NATIVE_COMPACTION);

        FFICommonConfig params = createCompactionParams(job, tableProperties, region, awsConfig, runtime);

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
    private static FFICommonConfig createCompactionParams(CompactionJob job, TableProperties tableProperties,
            Region region, DataFusionAwsConfig awsConfig, jnr.ffi.Runtime runtime) {
        Schema schema = tableProperties.getSchema();
        FFICommonConfig params = new FFICommonConfig(runtime, Optional.ofNullable(awsConfig));
        params.input_files.populate(job.getInputFiles().toArray(new String[0]), false);
        // Files are always sorted for compactions
        params.input_files_sorted.set(true);
        params.output_file.set(job.getOutputFile());
        params.row_key_cols.populate(schema.getRowKeyFieldNames().toArray(new String[0]), false);
        params.row_key_schema.populate(FFICommonConfig.getKeyTypes(schema.getRowKeyTypes()), false);
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
        if (DataEngine.AGGREGATION_ITERATOR_NAME.equals(job.getIteratorClassName())) {
            params.iterator_config.set(job.getIteratorConfig());
        } else {
            params.iterator_config.set("");
        }
        FFISleeperRegion partitionRegion = new FFISleeperRegion(runtime, region);
        params.setRegion(partitionRegion);
        params.validate();
        return params;
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
            FFICommonConfig compactionParams, jnr.ffi.Runtime runtime) throws IOException {
        // Create object to hold the result (in native memory)
        FFICompactionResult compactionData = new FFICompactionResult(runtime);
        // Perform compaction
        try (FFIContext context = new FFIContext(NATIVE_COMPACTION)) {
            int result = NATIVE_COMPACTION.compact(context, compactionParams, compactionData);
            // Check result
            if (result != 0) {
                LOGGER.error("DataFusion compaction failed, return code: {}", result);
                throw new IOException("DataFusion compaction failed with return code " + result);
            }
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
}
