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
import sleeper.query.datafusion.DataFusionQueryFunctionsImpl.LoadFailureTracker;

import java.util.function.Supplier;

/**
 * A wrapper for the DataFusion FFI context. Allows for an unimplemented row retriever if DataFusion could not be
 * loaded.
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
     * and Arrow allocator will be closed when the query context is closed.
     *
     * @param  allocator a constructor for the Arrow allocator
     * @return           the context
     */
    public static DataFusionQueryContext createIfPresent(Supplier<BufferAllocator> allocator) {
        LoadFailureTracker tracker = DataFusionQueryFunctionsImpl.getLoadFailureTracker();
        if (tracker.isFailed()) {
            return new DataFusionQueryContext(null, null);
        } else {
            return new DataFusionQueryContext(new FFIContext<>(tracker.getFunctionsOrThrow()), allocator.get());
        }
    }

    /**
     * Creates a row retriever provider for use in queries. If the DataFusion functions could not be loaded, attempting
     * to create a row retriever from this will throw a NotImplementedException.
     *
     * @param  awsConfig a constructor for the AWS configuration
     * @return           the row retriever provider
     */
    public LeafPartitionRowRetrieverProvider createRowRetrieverProvider(Supplier<DataFusionAwsConfig> awsConfig) {
        if (context == null) {
            return LeafPartitionRowRetrieverProvider.notImplemented("DataFusion not loaded");
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
