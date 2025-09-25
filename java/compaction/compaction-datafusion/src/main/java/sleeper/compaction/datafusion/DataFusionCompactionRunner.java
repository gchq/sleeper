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
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.compaction.datafusion.DataFusionFunctions.DataFusionCommonConfig;
import sleeper.compaction.datafusion.DataFusionFunctions.DataFusionCompactionResult;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.foreign.FFISleeperRegion;
import sleeper.foreign.bridge.FFIContext;
import sleeper.parquet.row.ParquetRowWriterFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

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

    /** Maximum number of rows in a Parquet row group. */
    public static final long DATAFUSION_MAX_ROW_GROUP_ROWS = 1_000_000;

    private final DataFusionAwsConfig awsConfig;
    private final Configuration hadoopConf;

    public DataFusionCompactionRunner(Configuration hadoopConf) {
        this(DataFusionAwsConfig.getDefault(), hadoopConf);
    }

    public DataFusionCompactionRunner(DataFusionAwsConfig awsConfig, Configuration hadoopConf) {
        this.awsConfig = awsConfig;
        this.hadoopConf = hadoopConf;
    }

    @Override
    public RowsProcessed compact(CompactionJob job, TableProperties tableProperties, Region region) throws IOException {
        jnr.ffi.Runtime runtime = jnr.ffi.Runtime.getRuntime(DataFusionFunctions.INSTANCE);

        DataFusionCommonConfig params = createCompactionParams(job, tableProperties, region, awsConfig, runtime);

        RowsProcessed result = invokeDataFusion(job, params, runtime);

        if (result.getRowsWritten() < 1) {
            try (ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(
                    new Path(job.getOutputFile()), tableProperties, hadoopConf)) {
                // Write an empty file. This should be temporary, as we expect DataFusion to add support for this.
                // See the test should_merge_empty_files in compaction_test.rs
            }
        }

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
    @SuppressWarnings(value = "checkstyle:avoidNestedBlocks")
    private static DataFusionCommonConfig createCompactionParams(CompactionJob job, TableProperties tableProperties,
            Region region, DataFusionAwsConfig awsConfig, jnr.ffi.Runtime runtime) {
        Schema schema = tableProperties.getSchema();
        DataFusionCommonConfig params = new DataFusionCommonConfig(runtime);
        if (awsConfig != null) {
            params.override_aws_config.set(true);
            params.aws_region.set(awsConfig.getRegion());
            params.aws_endpoint.set(awsConfig.getEndpoint());
            params.aws_allow_http.set(awsConfig.isAllowHttp());
            params.aws_access_key.set(awsConfig.getAccessKey());
            params.aws_secret_key.set(awsConfig.getSecretKey());
        } else {
            params.override_aws_config.set(false);
        }
        params.input_files.populate(job.getInputFiles().toArray(new String[0]), false);
        // Files are always sorted for compactions
        params.input_files_sorted.set(true);
        params.output_file.set(job.getOutputFile());
        params.row_key_cols.populate(schema.getRowKeyFieldNames().toArray(new String[0]), false);
        params.row_key_schema.populate(getKeyTypes(schema.getRowKeyTypes()), false);
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
        params.iterator_config.set("");
        params.aggregation_config.set(job.getAggregationConfig() == null ? "" : job.getAggregationConfig());
        params.filtering_config.set(job.getFilterConfig() == null ? "" : job.getFilterConfig());

        FFISleeperRegion partitionRegion = new FFISleeperRegion(runtime);
        List<Range> orderedRanges = region.getRangesOrdered(schema);
        // Extra braces: Make sure wrong array isn't populated to wrong pointers
        {
            // This array can't contain nulls
            Object[] regionMins = orderedRanges.stream().map(Range::getMin).toArray();
            partitionRegion.region_mins.populate(regionMins, false);
        }
        {
            Boolean[] regionMinInclusives = orderedRanges.stream().map(Range::isMinInclusive)
                    .toArray(Boolean[]::new);
            partitionRegion.region_mins_inclusive.populate(regionMinInclusives, false);
        }
        {
            // This array can contain nulls
            Object[] regionMaxs = orderedRanges.stream().map(Range::getMax).toArray();
            partitionRegion.region_maxs.populate(regionMaxs, true);
        }
        {
            Boolean[] regionMaxInclusives = orderedRanges.stream().map(Range::isMaxInclusive)
                    .toArray(Boolean[]::new);
            partitionRegion.region_maxs_inclusive.populate(regionMaxInclusives, false);
        }
        params.setRegion(partitionRegion);
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
     * Take the compaction parameters and invoke the DataFusion compactor using the FFI
     * bridge.
     *
     * @param  job              the compaction job
     * @param  compactionParams the compaction input parameters
     * @param  runtime          the JNR FFI runtime object
     * @return                  rows read/written
     * @throws IOException      if the foreign library call doesn't complete successfully
     */
    private static RowsProcessed invokeDataFusion(CompactionJob job,
            DataFusionCommonConfig compactionParams, jnr.ffi.Runtime runtime) throws IOException {
        // Create object to hold the result (in native memory)
        DataFusionCompactionResult compactionData = new DataFusionCompactionResult(runtime);
        // Perform compaction
        try (FFIContext context = new FFIContext(DataFusionFunctions.INSTANCE)) {
            int result = DataFusionFunctions.INSTANCE.compact(context, compactionParams, compactionData);
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
}
