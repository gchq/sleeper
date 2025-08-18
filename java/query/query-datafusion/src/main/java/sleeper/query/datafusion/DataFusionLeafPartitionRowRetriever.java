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

import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.foreign.FFISleeperRegion;
import sleeper.foreign.bridge.FFIBridge;
import sleeper.foreign.datafusion.DataFusionAwsConfig;
import sleeper.foreign.datafusion.FFICommonConfig;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetriever;
import sleeper.query.core.rowretrieval.RowRetrievalException;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.Collectors;

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
        throw new NotImplementedException();
    }

    /**
     * Creates the input struct that contains all the information needed by the Rust code
     * side of the compaction.
     *
     * This includes all Parquet writer settings as well as compaction data such as
     * input files, compaction
     * region etc.
     *
     * @param  query     all details for this leaf partition query
     * @param  awsConfig settings to access AWS, or null to use defaults
     * @param  runtime   FFI runtime
     * @return           object to pass to FFI layer
     */
    private static FFILeafPartitionQueryConfig createFFIQueryData(LeafPartitionQuery query, Schema dataReadSchema, DataFusionAwsConfig awsConfig, jnr.ffi.Runtime runtime) {
        FFICommonConfig common = new FFICommonConfig(runtime, Optional.ofNullable(awsConfig));
        common.input_files.populate(query.getFiles().toArray(new String[0]), false);
        // Files are always sorted for queries
        common.input_files_sorted.set(true);
        common.file_output_enabled.set(false);
        common.row_key_cols.populate(dataReadSchema.getRowKeyFieldNames().toArray(new String[0]), false);
        common.row_key_schema.populate(FFICommonConfig.getKeyTypes(dataReadSchema.getRowKeyTypes()), false);
        common.sort_key_cols.populate(dataReadSchema.getSortKeyFieldNames().toArray(new String[0]), false);
        // Is there an aggregation/filtering iterator set?
        if (DataEngine.AGGREGATION_ITERATOR_NAME.equals(query.getQueryTimeIteratorClassName())) {
            common.iterator_config.set(query.getQueryTimeIteratorConfig());
        } else {
            common.iterator_config.set("");
        }
        FFISleeperRegion partitionRegion = new FFISleeperRegion(runtime, query.getPartitionRegion());
        common.setRegion(partitionRegion);
        common.validate();

        FFILeafPartitionQueryConfig queryConfig = new FFILeafPartitionQueryConfig(runtime);
        queryConfig.common.set(common);
        FFISleeperRegion[] ffi_regions = query.getRegions().stream().map(region -> new FFISleeperRegion(runtime, region)).collect(Collectors.toList()).toArray(FFISleeperRegion[]::new);
        queryConfig.setQueryRegions(ffi_regions);
        queryConfig.write_quantile_sketch.set(false);
        queryConfig.explain_plans.set(true);
        return queryConfig;
    }=
}
