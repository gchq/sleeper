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
package sleeper.systemtest.drivers.util;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.foreign.bridge.FFIContext;
import sleeper.foreign.datafusion.DataFusionAwsConfig;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetrieverProvider;
import sleeper.query.datafusion.DataFusionLeafPartitionRowRetriever;
import sleeper.query.datafusion.DataFusionQueryFunctions;
import sleeper.query.datafusion.DataFusionQueryFunctionsImpl;

import java.util.function.Supplier;

public class SystemTestDataFusion {

    public static final Logger LOGGER = LoggerFactory.getLogger(SystemTestDataFusion.class);
    private static final DataFusionQueryFunctions QUERY_FUNCTIONS = createQueryFunctionsOrNull();
    private static final FFIContext<DataFusionQueryFunctions> QUERY_CONTEXT = QUERY_FUNCTIONS == null ? null : new FFIContext<>(QUERY_FUNCTIONS);
    private static final BufferAllocator ALLOCATOR = QUERY_CONTEXT == null ? null : new RootAllocator();

    public static LeafPartitionRowRetrieverProvider createRowRetrieverProvider(Supplier<DataFusionAwsConfig> awsConfig) {
        if (QUERY_CONTEXT == null) {
            return LeafPartitionRowRetrieverProvider.notImplemented("DataFusion not loaded");
        } else {
            return new DataFusionLeafPartitionRowRetriever.Provider(awsConfig.get(), ALLOCATOR, QUERY_CONTEXT);
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
