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
package sleeper.query.datafusion;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.row.Row;
import sleeper.core.rowbatch.arrow.RowIteratorFromArrowReader;
import sleeper.core.schema.Schema;
import sleeper.foreign.FFISleeperRegion;
import sleeper.foreign.bridge.FFIBridge;
import sleeper.foreign.bridge.FFIContext;
import sleeper.foreign.datafusion.DataFusionAwsConfig;
import sleeper.foreign.datafusion.FFICommonConfig;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetriever;
import sleeper.query.core.rowretrieval.RowRetrievalException;

import java.io.IOException;
import java.util.Optional;

/**
 * Implements a Sleeper row retriever based on Apache DataFusion using native code.
 */
public class DataFusionLeafPartitionRowRetriever implements LeafPartitionRowRetriever {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataFusionLeafPartitionRowRetriever.class);

    private static final DataFusionQueryFunctions NATIVE_QUERY;

    private final DataFusionAwsConfig awsConfig;

    static {
        // Obtain native library. This throws an exception if native library can't be
        // loaded and linked
        try {
            NATIVE_QUERY = FFIBridge.createForeignInterface(DataFusionQueryFunctions.class);
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public DataFusionLeafPartitionRowRetriever() {
        this(DataFusionAwsConfig.getDefault());
    }

    public DataFusionLeafPartitionRowRetriever(DataFusionAwsConfig awsConfig) {
        this.awsConfig = awsConfig;
    }

    @Override
    public CloseableIterator<Row> getRows(LeafPartitionQuery leafPartitionQuery, Schema dataReadSchema) throws RowRetrievalException {
        jnr.ffi.Runtime runtime = jnr.ffi.Runtime.getRuntime(NATIVE_QUERY);
        FFILeafPartitionQueryConfig params = createFFIQueryData(leafPartitionQuery, dataReadSchema, awsConfig, runtime);

        FFIQueryResults results = new FFIQueryResults(runtime);
        // Perform native query
        try (FFIContext context = new FFIContext(NATIVE_QUERY)) {
            // Create NULL pointer which will be set by the FFI call upon return
            int result = NATIVE_QUERY.query(context, params, results);
            // Check result
            if (result != 0) {
                LOGGER.error("DataFusion query failed, return code: {}", result);
                throw new RowRetrievalException("DataFusion query failed with return code " + result);
            }

            BufferAllocator alloc = new RootAllocator();
            // Convert pointer from Rust to Java FFI Arrow array stream.
            // At this point Java assumes ownership of the stream and must release it when no longer
            // needed.
            CloseableIterator<Row> rowConversion = new RowIteratorFromArrowReader(Data.importArrayStream(alloc, ArrowArrayStream.wrap(results.arrowArrayStreamPtr.longValue())));

            return new CloseableIterator<Row>() {

                @Override
                public boolean hasNext() {
                    return rowConversion.hasNext();
                }

                @Override
                public Row next() {
                    return rowConversion.next();
                }

                @Override
                public void close() throws IOException {
                    rowConversion.close();
                    alloc.close();
                }
            };
        } catch (IOException e) {
            throw new RowRetrievalException(e.getMessage(), e);
        }
    }

    /**
     * Creates the input struct that contains all the information needed by the Rust code
     * side of the compaction.
     *
     * This includes all Parquet writer settings as well as compaction data such as
     * input files, compaction
     * region etc.
     *
     * @param  query          all details for this leaf partition query
     * @param  dataReadSchema the input schema to read
     * @param  awsConfig      settings to access AWS, or null to use defaults
     * @param  runtime        FFI runtime
     * @return                object to pass to FFI layer
     */
    private static FFILeafPartitionQueryConfig createFFIQueryData(LeafPartitionQuery query, Schema dataReadSchema, DataFusionAwsConfig awsConfig, jnr.ffi.Runtime runtime) {
        FFICommonConfig common = new FFICommonConfig(runtime, Optional.ofNullable(awsConfig));
        common.input_files.populate(query.getFiles().toArray(String[]::new), false);
        // Files are always sorted for queries
        common.input_files_sorted.set(true);
        common.row_key_cols.populate(dataReadSchema.getRowKeyFieldNames().toArray(String[]::new), false);
        common.row_key_schema.populate(FFICommonConfig.getKeyTypes(dataReadSchema.getRowKeyTypes()), false);
        common.sort_key_cols.populate(dataReadSchema.getSortKeyFieldNames().toArray(String[]::new), false);
        // Is there an aggregation/filtering iterator set?
        if (DataEngine.AGGREGATION_ITERATOR_NAME.equals(query.getQueryTimeIteratorClassName())) {
            common.iterator_config.set(query.getQueryTimeIteratorConfig());
        }
        FFISleeperRegion partitionRegion = new FFISleeperRegion(runtime, query.getPartitionRegion());
        common.setRegion(partitionRegion);
        common.validate();

        FFILeafPartitionQueryConfig queryConfig = new FFILeafPartitionQueryConfig(runtime);
        queryConfig.common.set(common);
        FFISleeperRegion[] ffiRegions = query.getRegions().stream().map(region -> new FFISleeperRegion(runtime, region)).toArray(FFISleeperRegion[]::new);
        queryConfig.setQueryRegions(ffiRegions);
        queryConfig.write_quantile_sketch.set(false);
        queryConfig.explain_plans.set(true);
        return queryConfig;
    }
}
