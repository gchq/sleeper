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

public class DataFusionQueryContext implements AutoCloseable {

    public static final Logger LOGGER = LoggerFactory.getLogger(DataFusionQueryContext.class);

    private static final DataFusionQueryFunctions FUNCTIONS = createQueryFunctionsOrNull();

    private final FFIContext<DataFusionQueryFunctions> context;
    private final BufferAllocator allocator;

    private DataFusionQueryContext(FFIContext<DataFusionQueryFunctions> context, BufferAllocator allocator) {
        this.context = context;
        this.allocator = allocator;
    }

    public static DataFusionQueryContext createIfPresent(Supplier<BufferAllocator> allocator) {
        if (FUNCTIONS != null) {
            return new DataFusionQueryContext(new FFIContext<>(FUNCTIONS), allocator.get());
        } else {
            return new DataFusionQueryContext(null, null);
        }
    }

    @Override
    public void close() throws Exception {
        if (context != null) {
            try (context; allocator) {
            }
        }
    }

    public LeafPartitionRowRetrieverProvider createRowRetrieverProvider(Supplier<DataFusionAwsConfig> awsConfig) {
        if (context == null) {
            return LeafPartitionRowRetrieverProvider.notImplemented("DataFusion not loaded");
        } else {
            return new DataFusionLeafPartitionRowRetriever.Provider(awsConfig.get(), allocator, context);
        }
    }

    private static DataFusionQueryFunctions createQueryFunctionsOrNull() {
        try {
            return DataFusionQueryFunctionsImpl.create();
        } catch (RuntimeException e) {
            LOGGER.error("Failed to load query functions implementation", e);
            return null;
        }
    }
}
