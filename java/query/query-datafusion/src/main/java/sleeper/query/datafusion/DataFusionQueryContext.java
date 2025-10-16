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
import sleeper.query.core.rowretrieval.QueryEngineSelector;

import java.util.Optional;
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
     * this allows falling back to the Java implementation.
     *
     * @param  allocator a constructor for the Arrow allocator
     * @return           the context
     */
    public static DataFusionQueryContext createIfLoaded(Supplier<BufferAllocator> allocator) {
        Optional<DataFusionQueryFunctions> functions = DataFusionQueryFunctionsImpl.getInstanceIfLoaded();
        if (functions.isPresent()) {
            return new DataFusionQueryContext(new FFIContext<>(functions.get()), allocator.get());
        } else {
            return new DataFusionQueryContext(null, null);
        }
    }

    /**
     * Creates a row retriever provider for use in queries, using the Java or DataFusion implementation depending on
     * configuration. If the DataFusion functions could not be loaded, the Java implementation will always be used.
     *
     * @param  awsConfig    a constructor for the AWS configuration
     * @param  javaProvider the Java implementation
     * @return              the row retriever provider
     */
    public LeafPartitionRowRetrieverProvider createQueryEngineSelector(Supplier<DataFusionAwsConfig> awsConfig, LeafPartitionRowRetrieverProvider javaProvider) {
        if (context == null) {
            LOGGER.warn("Falling back to Java row retriever as DataFusion was not loaded");
            return javaProvider;
        } else {
            return QueryEngineSelector.javaAndDataFusion(javaProvider,
                    new DataFusionLeafPartitionRowRetriever.Provider(awsConfig.get(), allocator, context));
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
