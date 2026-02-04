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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Region;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.foreign.FFIFileResult;
import sleeper.foreign.FFISleeperRegion;
import sleeper.foreign.bridge.FFIContext;
import sleeper.foreign.datafusion.DataFusionAwsConfig;
import sleeper.foreign.datafusion.FFICommonConfig;
import sleeper.parquet.row.ParquetRowWriterFactory;

import java.io.IOException;
import java.time.LocalDateTime;

import static sleeper.core.properties.table.TableProperty.COLUMN_INDEX_TRUNCATE_LENGTH;
import static sleeper.core.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.core.properties.table.TableProperty.DATAFUSION_S3_READAHEAD_ENABLED;
import static sleeper.core.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS;
import static sleeper.core.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS;
import static sleeper.core.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_VALUE_FIELDS;
import static sleeper.core.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.core.properties.table.TableProperty.PARQUET_ROW_GROUP_SIZE_ROWS;
import static sleeper.core.properties.table.TableProperty.PARQUET_WRITER_VERSION;
import static sleeper.core.properties.table.TableProperty.STATISTICS_TRUNCATE_LENGTH;

@SuppressFBWarnings("UUF_UNUSED_FIELD")
public class DataFusionCompactionRunner implements CompactionRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataFusionCompactionRunner.class);

    private final DataFusionAwsConfig awsConfig;
    private final Configuration hadoopConf;
    private final FFIContext<DataFusionCompactionFunctions> context;

    public DataFusionCompactionRunner(DataFusionAwsConfig awsConfig, Configuration hadoopConf,
            FFIContext<DataFusionCompactionFunctions> context) {
        this.awsConfig = awsConfig;
        this.hadoopConf = hadoopConf;
        this.context = context;
    }

    @Override
    public RowsProcessed compact(CompactionJob job, TableProperties tableProperties, Region region) throws IOException {
        jnr.ffi.Runtime runtime = jnr.ffi.Runtime.getRuntime(context.getFunctions());
        FFICommonConfig params = createCompactionParams(job, tableProperties, region, awsConfig, runtime);
        RowsProcessed result = invokeDataFusion(job, params, runtime, context);

        // Get the filesystem object
        FileSystem fs = FileSystem.get(hadoopConf);
        Path outputPath = new Path(job.getOutputFile());

        if (result.getRowsWritten() < 1 && !fs.exists(outputPath)) {
            try (ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(
                    outputPath, tableProperties, hadoopConf)) {
                // Write an empty file. This should be temporary, as we expect DataFusion to add
                // support for this.
                // See the test should_merge_empty_files in compaction_test.rs
            }
        }

        LOGGER.info("Compaction job {}: compaction finished at {}", job.getId(),
                LocalDateTime.now());
        return result;
    }

    /**
     * Creates the input struct that contains all the information needed by the Rust
     * code side of the compaction.
     *
     * This includes all Parquet writer settings as well as compaction data such as
     * input files, compaction region etc.
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
        FFICommonConfig params = new FFICommonConfig(runtime, awsConfig);
        params.input_files.populate(job.getInputFiles().toArray(String[]::new), false);
        // Files are always sorted for compactions
        params.input_files_sorted.set(true);
        params.use_readahead_store.set(tableProperties.getBoolean(DATAFUSION_S3_READAHEAD_ENABLED));
        // Reading page indexes are not useful for compactions
        params.read_page_indexes.set(false);
        params.output_file.set(job.getOutputFile());
        params.write_sketch_file.set(true);
        params.row_key_cols.populate(schema.getRowKeyFieldNames().toArray(String[]::new), false);
        params.row_key_schema.populate(FFICommonConfig.getKeyTypes(schema.getRowKeyTypes()), false);
        params.sort_key_cols.populate(schema.getSortKeyFieldNames().toArray(String[]::new), false);
        params.max_row_group_size.set(tableProperties.getInt(PARQUET_ROW_GROUP_SIZE_ROWS));
        params.max_page_size.set(tableProperties.getInt(PAGE_SIZE));
        params.compression.set(tableProperties.get(COMPRESSION_CODEC));
        params.writer_version.set(tableProperties.get(PARQUET_WRITER_VERSION));
        params.column_truncate_length.set(tableProperties.getInt(COLUMN_INDEX_TRUNCATE_LENGTH));
        params.stats_truncate_length.set(tableProperties.getInt(STATISTICS_TRUNCATE_LENGTH));
        params.dict_enc_row_keys.set(tableProperties.getBoolean(DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS));
        params.dict_enc_sort_keys.set(tableProperties.getBoolean(DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS));
        params.dict_enc_values.set(tableProperties.getBoolean(DICTIONARY_ENCODING_FOR_VALUE_FIELDS));
        params.aggregation_config.set(job.getAggregationConfig() == null ? "" : job.getAggregationConfig());
        params.filtering_config.set(job.getFilterConfig() == null ? "" : job.getFilterConfig());
        params.region.set(FFISleeperRegion.from(region, schema, runtime));
        params.validate();

        return params;
    }

    /**
     * Take the compaction parameters and invoke the DataFusion compactor using the FFI bridge.
     *
     * @param  job              the compaction job
     * @param  compactionParams the compaction input parameters
     * @param  runtime          the JNR FFI runtime object
     * @param  context          the open context for FFI calls
     * @return                  rows read/written
     * @throws IOException      if the foreign library call doesn't complete successfully
     */
    private static RowsProcessed invokeDataFusion(CompactionJob job, FFICommonConfig compactionParams,
            jnr.ffi.Runtime runtime, FFIContext<DataFusionCompactionFunctions> context) throws IOException {
        // Create object to hold the result (in native memory)
        FFIFileResult compactionData = new FFIFileResult(runtime);
        // Perform compaction

        int result = context.getFunctions().compact(context, compactionParams, compactionData);
        // Check result
        if (result != 0) {
            LOGGER.error("DataFusion compaction failed, return code: {}", result);
            throw new IOException("DataFusion compaction failed with return code " + result);
        }

        long totalNumberOfRowsRead = compactionData.rows_read.get();
        long rowsWritten = compactionData.rows_written.get();

        LOGGER.info("Compaction job {}: Read {} rows and wrote {} rows",
                job.getId(), totalNumberOfRowsRead, rowsWritten);

        return new RowsProcessed(totalNumberOfRowsRead, rowsWritten);
    }
}
