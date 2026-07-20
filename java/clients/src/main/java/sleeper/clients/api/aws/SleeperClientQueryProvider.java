/*
 * Copyright 2022-2026 Crown Copyright
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

import sleeper.clients.util.ShutdownWrapper;
import sleeper.clients.util.UncheckedAutoCloseable;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.foreign.datafusion.DataFusionAwsConfig;
import sleeper.parquet.utils.TableHadoopConfigurationProvider;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetrieverProvider;
import sleeper.query.core.rowretrieval.QueryEngineSelector;
import sleeper.query.datafusion.PerCallDataFusionRowRetrieverProvider;
import sleeper.query.runner.rowretrieval.LeafPartitionRowRetrieverImpl;

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
     * @param  instanceProperties Sleeper instance properties
     * @return                    the provider
     */
    static SleeperClientQueryProvider createDefaultForEachClient(InstanceProperties instanceProperties) {
        return withThreadPoolForEachClient(instanceProperties, 10);
    }

    /**
     * Creates a provider that will create a new thread pool for each Sleeper client, that will be closed when the
     * Sleeper client is closed.
     *
     * @param  instanceProperties Sleeper instance properties
     * @param  threadPoolSize     the number of threads in the thread pool for each client
     * @return                    the provider
     */
    static SleeperClientQueryProvider withThreadPoolForEachClient(InstanceProperties instanceProperties, int threadPoolSize) {
        return hadoopProvider -> {
            ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);
            PerCallDataFusionRowRetrieverProvider dataFusionProvider = new PerCallDataFusionRowRetrieverProvider(DataFusionAwsConfig.getDefault(instanceProperties));

            LeafPartitionRowRetrieverProvider javaProvider = new LeafPartitionRowRetrieverImpl.Provider(executorService, hadoopProvider);
            return ShutdownWrapper.shutdown(
                    QueryEngineSelector.javaAndDataFusion(javaProvider, dataFusionProvider),
                    (Runnable) executorService::shutdown);
        };
    }

    /**
     * Creates a provider backed by one thread pool. Please ensure the returned provider is closed.
     *
     * @param  instanceProperties Sleeper instance properties
     * @param  threadPoolSize     the number of threads in the thread pool
     * @return                    the provider
     */
    static PersistentThreadPool withPersistentThreadPool(InstanceProperties instanceProperties, int threadPoolSize) {
        return new PersistentThreadPool(instanceProperties, Executors.newFixedThreadPool(threadPoolSize));
    }

    /**
     * A query provider backed by a single thread pool.
     */
    class PersistentThreadPool implements SleeperClientQueryProvider, UncheckedAutoCloseable {
        private final ExecutorService executorService;
        private final PerCallDataFusionRowRetrieverProvider dataFusionProvider;

        private PersistentThreadPool(InstanceProperties instanceProperties, ExecutorService executorService) {
            this.executorService = executorService;
            this.dataFusionProvider = new PerCallDataFusionRowRetrieverProvider(DataFusionAwsConfig.getDefault(instanceProperties));
        }

        @Override
        public ShutdownWrapper<LeafPartitionRowRetrieverProvider> getRowRetrieverProvider(TableHadoopConfigurationProvider hadoopProvider) {
            LeafPartitionRowRetrieverProvider javaProvider = new LeafPartitionRowRetrieverImpl.Provider(executorService, hadoopProvider);
            return ShutdownWrapper.noShutdown(QueryEngineSelector.javaAndDataFusion(javaProvider, dataFusionProvider));
        }

        @Override
        public void close() {
            executorService.shutdown();
        }
    }
}
