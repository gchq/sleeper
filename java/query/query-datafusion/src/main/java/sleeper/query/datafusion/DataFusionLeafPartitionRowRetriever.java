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

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;
import sleeper.core.rowbatch.arrow.RowIteratorFromArrowReader;
import sleeper.core.schema.Schema;
import sleeper.foreign.FFISleeperRegion;
import sleeper.foreign.bridge.FFIContext;
import sleeper.foreign.datafusion.DataFusionAwsConfig;
import sleeper.foreign.datafusion.FFICommonConfig;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetriever;
import sleeper.query.core.rowretrieval.RowRetrievalException;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.util.Optional;

/**
 * Implements a Sleeper row retriever based on Apache DataFusion using native code.
 */
public class DataFusionLeafPartitionRowRetriever implements LeafPartitionRowRetriever {
    private static final Cleaner CLEANER = Cleaner.create();
    private static final Logger LOGGER = LoggerFactory.getLogger(DataFusionLeafPartitionRowRetriever.class);

    private final DataFusionAwsConfig awsConfig;
    private final BufferAllocator allocator;
    private final FFIContext context;

    private DataFusionLeafPartitionRowRetriever(Builder builder) {
        this.awsConfig = builder.awsConfig;
        this.allocator = Optional.ofNullable(builder.allocator).orElseGet(() -> createAllocator(this));
        this.context = Optional.ofNullable(builder.context).orElseGet(() -> createContext(this));
    }

    private static BufferAllocator createAllocator(Object instance) {
        RootAllocator allocator = new RootAllocator();
        CLEANER.register(instance, allocator::close);
        return allocator;
    }

    private static FFIContext createContext(Object instance) {
        FFIContext context = createContext();
        CLEANER.register(instance, context::close);
        return context;
    }

    /**
     * Creates an FFI context for reuse.
     *
     * @return the context
     */
    public static FFIContext createContext() {
        return new FFIContext(DataFusionQueryFunctions.INSTANCE);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public CloseableIterator<Row> getRows(LeafPartitionQuery leafPartitionQuery, Schema dataReadSchema) throws RowRetrievalException {
        jnr.ffi.Runtime runtime = jnr.ffi.Runtime.getRuntime(DataFusionQueryFunctions.INSTANCE);
        FFILeafPartitionQueryConfig params = createFFIQueryData(leafPartitionQuery, dataReadSchema, awsConfig, runtime);

        FFIQueryResults results = new FFIQueryResults(runtime);
        // Perform native query
        try {
            // Create NULL pointer which will be set by the FFI call upon return
            int result = DataFusionQueryFunctions.INSTANCE.query_stream(context, params, results);
            // Check result
            if (result != 0) {
                LOGGER.error("DataFusion query failed, return code: {}", result);
                throw new RowRetrievalException("DataFusion query failed with return code " + result);
            }

            // A zeroed (NULL) results pointer is NEVER correct here
            if (results.arrowArrayStream.longValue() == 0) {
                throw new RowRetrievalException("Call to DataFusion query layer returned a NULL pointer");
            }

            // Convert pointer from Rust to Java FFI Arrow array stream.
            // At this point Java assumes ownership of the stream and must release it when no longer
            // needed.
            return new RowIteratorFromArrowReader(Data.importArrayStream(allocator, ArrowArrayStream.wrap(results.arrowArrayStream.longValue())));
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
        FFICommonConfig common = new FFICommonConfig(runtime, awsConfig);
        common.input_files.populate(query.getFiles().toArray(String[]::new), false);
        // Files are always sorted for queries
        common.input_files_sorted.set(true);
        common.row_key_cols.populate(dataReadSchema.getRowKeyFieldNames().toArray(String[]::new), false);
        common.row_key_schema.populate(FFICommonConfig.getKeyTypes(dataReadSchema.getRowKeyTypes()), false);
        common.sort_key_cols.populate(dataReadSchema.getSortKeyFieldNames().toArray(String[]::new), false);
        common.region.set(FFISleeperRegion.from(query.getPartitionRegion(), dataReadSchema, runtime));
        common.write_sketch_file.set(false);
        common.aggregation_config.set("");
        common.filtering_config.set("");
        common.validate();

        FFILeafPartitionQueryConfig queryConfig = new FFILeafPartitionQueryConfig(runtime);
        queryConfig.common.set(common);

        // Copying logic in LeafPartitionQueryExecutor#createSchemaForDataRead, we see if the query has any
        // requested value fields, if it does, grab the value fields from the dataReadSchema, since this
        // will include extra value fields needed for Java iterators to execute
        if (query.getRequestedValueFields() != null) {
            queryConfig.requested_value_fields.populate(dataReadSchema.getValueFieldNames().toArray(String[]::new), false);
            queryConfig.requested_value_fields_set.set(true);
        } else {
            queryConfig.requested_value_fields_set.set(false);
        }

        FFISleeperRegion[] ffiRegions = query.getRegions().stream().map(region -> FFISleeperRegion.from(region, dataReadSchema, runtime)).toArray(FFISleeperRegion[]::new);
        queryConfig.setQueryRegions(ffiRegions);
        queryConfig.explain_plans.set(true);

        queryConfig.validate();
        return queryConfig;
    }

    /**
     * Builder for creating instances of outer class.
     *
     * This builder allows configuration of:
     * <ul>
     * <li>An optional object for AWS integration</li>
     * <li>An optional object for Apache Arrow memory management</li>
     * </ul>
     *
     * If no allocator is explicitly provided via {@link #allocator(BufferAllocator)},
     * a new {@link RootAllocator} will be created when {@link #build()} is called.
     *
     * Example usage:
     *
     * <pre>
     * DataFusionLeafPartitionRowRetriever retriever = DataFusionLeafPartitionRowRetriever.builder()
     *         .awsConfig(myAwsConfig)
     *         // .allocator(customAllocator) // Optional — defaults to new RootAllocator()
     *         .build();
     * </pre>
     */
    public static class Builder {
        private DataFusionAwsConfig awsConfig = DataFusionAwsConfig.getDefault();
        private BufferAllocator allocator;
        private FFIContext context;

        /**
         * Sets the AWS configuration for the retriever.
         *
         * @param  awsConfig the {@link DataFusionAwsConfig} to use
         * @return           this builder for method chaining
         */
        public Builder awsConfig(DataFusionAwsConfig awsConfig) {
            this.awsConfig = awsConfig;
            return this;
        }

        /**
         * Sets the Apache Arrow allocator for the retriever.
         *
         * If not set, a {@link RootAllocator} will be created automatically during build.
         *
         * @param  allocator the allocator to use
         * @return           this builder for method chaining
         */
        public Builder allocator(BufferAllocator allocator) {
            this.allocator = allocator;
            return this;
        }

        /**
         * Sets the FFI context.
         *
         * If not set, it will be created automatically during build.
         *
         * @param  context the context to use; may be {@code null} to accept the default
         * @return         this builder for method chaining
         */
        public Builder context(FFIContext context) {
            this.context = context;
            return this;
        }

        /**
         * Builds a new instance using the configuration provided to this builder.
         *
         * @return a fully constructed {@link DataFusionLeafPartitionRowRetriever}
         */
        public DataFusionLeafPartitionRowRetriever build() {
            return new DataFusionLeafPartitionRowRetriever(this);
        }
    }
}
