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
package sleeper.clients.api.aws;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import sleeper.clients.util.ShutdownWrapper;
import sleeper.clients.util.UncheckedAutoCloseable;
import sleeper.clients.util.UncheckedAutoCloseables;
import sleeper.foreign.bridge.FFIContext;
import sleeper.foreign.datafusion.DataFusionAwsConfig;
import sleeper.parquet.utils.TableHadoopConfigurationProvider;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetrieverProvider;
import sleeper.query.core.rowretrieval.QueryEngineSelector;
import sleeper.query.datafusion.DataFusionLeafPartitionRowRetriever;
import sleeper.query.datafusion.DataFusionQueryFunctions;
import sleeper.query.runner.rowretrieval.LeafPartitionRowRetrieverImpl;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Provides row retrievers for running Sleeper queries with Hadoop.
 */
@FunctionalInterface
public interface SleeperClientQueryProvider {

    /**
     * Creates or retrieves a row retriever provider.
     *
     * @param  hadoopProvider the Hadoop configuration provider
     * @return                the row retriever
     */
    ShutdownWrapper<LeafPartitionRowRetrieverProvider> getRowRetrieverProvider(TableHadoopConfigurationProvider hadoopProvider);

    /**
     * Creates a provider that will create a thread pool of the default size. A new thread pool will be created for each
     * Sleeper client and closed when the Sleeper client is closed.
     *
     * @return the provider
     */
    static SleeperClientQueryProvider createDefaultForEachClient() {
        return withThreadPoolForEachClient(10);
    }

    /**
     * Creates a provider that will create a new thread pool for each Sleeper client, that will be closed when the
     * Sleeper client is closed.
     *
     * @param  threadPoolSize the number of threads in the thread pool for each client
     * @return                the provider
     */
    static SleeperClientQueryProvider withThreadPoolForEachClient(int threadPoolSize) {
        return hadoopProvider -> {
            ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);
            BufferAllocator allocator = new RootAllocator();
            FFIContext<DataFusionQueryFunctions> ffiContext = DataFusionLeafPartitionRowRetriever.createContext();
            LeafPartitionRowRetrieverProvider engineSelector = QueryEngineSelector.javaAndDataFusion(
                    new LeafPartitionRowRetrieverImpl.Provider(executorService, hadoopProvider),
                    new DataFusionLeafPartitionRowRetriever.Provider(DataFusionAwsConfig.getDefault(), allocator, ffiContext));
            UncheckedAutoCloseables shutdown = new UncheckedAutoCloseables(List.of(
                    allocator::close,
                    ffiContext::close,
                    executorService::shutdown));
            return ShutdownWrapper.shutdown(engineSelector, shutdown::close);
        };
    }

    /**
     * Creates a provider backed by one thread pool. Please ensure the returned provider is closed.
     *
     * @param  threadPoolSize the number of threads in the thread pool
     * @return                the provider
     */
    static PersistentThreadPool withPersistentThreadPool(int threadPoolSize) {
        return new PersistentThreadPool(Executors.newFixedThreadPool(threadPoolSize));
    }

    class PersistentThreadPool implements SleeperClientQueryProvider, UncheckedAutoCloseable {
        private final ExecutorService executorService;
        private final BufferAllocator allocator = new RootAllocator();
        private final FFIContext<DataFusionQueryFunctions> ffiContext = DataFusionLeafPartitionRowRetriever.createContext();

        private PersistentThreadPool(ExecutorService executorService) {
            this.executorService = executorService;
        }

        @Override
        public ShutdownWrapper<LeafPartitionRowRetrieverProvider> getRowRetrieverProvider(TableHadoopConfigurationProvider hadoopProvider) {
            LeafPartitionRowRetrieverProvider engineSelector = QueryEngineSelector.javaAndDataFusion(
                    new LeafPartitionRowRetrieverImpl.Provider(executorService, hadoopProvider),
                    new DataFusionLeafPartitionRowRetriever.Provider(DataFusionAwsConfig.getDefault(), allocator, ffiContext));
            return ShutdownWrapper.noShutdown(engineSelector);
        }

        @Override
        public void close() {
            try (allocator; ffiContext) {
            } finally {
                executorService.shutdown();
            }
        }

    }

}
