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

import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.foreign.bridge.FFIContext;
import sleeper.foreign.datafusion.DataFusionAwsConfig;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetrieverProvider;

import java.util.function.Supplier;

/**
 * A wrapper for the DataFusion FFI context. Allows for fallback if DataFusion could not be loaded.
 */
public class DataFusionQueryContext implements AutoCloseable {

    public static final Logger LOGGER = LoggerFactory.getLogger(DataFusionQueryContext.class);

    private final FFIContext<DataFusionQueryFunctions> context;
    private final BufferAllocator allocator;

    private DataFusionQueryContext(FFIContext<DataFusionQueryFunctions> context, BufferAllocator allocator) {
        this.context = context;
        this.allocator = allocator;
    }

    /**
     * Creates the DataFusion FFI context and Arrow allocator if the DataFusion functions were loaded. The FFI context
     * and Arrow allocator will be closed when the query context is closed. If the DataFusion functions were not loaded,
     * this avoids immediate failure in case you only want to use the Java data engine.
     *
     * @param  allocator a constructor for the Arrow allocator
     * @return           the context
     */
    public static DataFusionQueryContext createIfLoaded(Supplier<BufferAllocator> allocator) {
        return DataFusionQueryFunctions.getInstanceIfLoaded()
                .map(functions -> new DataFusionQueryContext(new FFIContext<>(functions), allocator.get()))
                .orElseGet(() -> none());
    }

    /**
     * Creates a context where no DataFusion functions are used. Supports cases where an instance of this class is
     * required but we will always use the Java data engine.
     *
     * @return the context
     */
    public static DataFusionQueryContext none() {
        return new DataFusionQueryContext(null, null);
    }

    /**
     * Creates a DataFusion row retriever provider for use in queries. If the DataFusion functions could not be loaded,
     * the returned row retriever will fail if it is used.
     *
     * @param  awsConfig a constructor for the AWS configuration
     * @return           the row retriever provider
     */
    public LeafPartitionRowRetrieverProvider createDataFusionProvider(Supplier<DataFusionAwsConfig> awsConfig) {
        if (context == null) {
            return tableProperties -> {
                throw new IllegalStateException("Could not load foreign interface", DataFusionQueryFunctions.getLoadingFailure());
            };
        } else {
            return new DataFusionLeafPartitionRowRetriever.Provider(awsConfig.get(), allocator, context);
        }
    }

    @Override
    public void close() {
        if (context != null) {
            try (context; allocator) {
            }
        }
    }
}
