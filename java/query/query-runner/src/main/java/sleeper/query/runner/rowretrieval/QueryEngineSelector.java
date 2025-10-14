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
package sleeper.query.runner.rowretrieval;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;

import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.table.TableProperties;
import sleeper.foreign.bridge.FFIContext;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetriever;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetrieverProvider;
import sleeper.query.datafusion.DataFusionLeafPartitionRowRetriever;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

import static sleeper.core.properties.table.TableProperty.DATA_ENGINE;

/**
 * Selects a query engine based on the data engine chosen for a table.
 */
public class QueryEngineSelector implements LeafPartitionRowRetrieverProvider {
    /** Executor service used to create Java based query code. */
    private final ExecutorService executorService;
    /** Hadoop configuration needed to Java based query code. */
    private final Configuration configuration;
    private final BufferAllocator arrowAllocator;
    private final FFIContext ffiContext;

    public QueryEngineSelector(ExecutorService executorService, Configuration configuration) {
        this(executorService, configuration, null, null);
    }

    public QueryEngineSelector(ExecutorService executorService, Configuration configuration, BufferAllocator arrowAllocator, FFIContext ffiContext) {
        this.executorService = Objects.requireNonNull(executorService, "executorService");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.arrowAllocator = arrowAllocator;
        this.ffiContext = ffiContext;
    }

    @Override
    public LeafPartitionRowRetriever getRowRetriever(TableProperties tableProperties) {
        DataEngine engine = tableProperties.getEnumValue(DATA_ENGINE, DataEngine.class);
        switch (engine) {
            case DATAFUSION_EXPERIMENTAL:
                return DataFusionLeafPartitionRowRetriever.builder()
                        .allocator(arrowAllocator)
                        .context(ffiContext)
                        .build();
            case DATAFUSION:
            case JAVA:
            default:
                return new LeafPartitionRowRetrieverImpl(executorService, configuration, tableProperties);
        }
    }
}
