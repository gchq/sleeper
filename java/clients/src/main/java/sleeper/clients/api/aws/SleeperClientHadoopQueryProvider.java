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

import org.apache.hadoop.conf.Configuration;

import sleeper.clients.util.ShutdownWrapper;
import sleeper.clients.util.UncheckedAutoCloseable;
import sleeper.query.core.recordretrieval.LeafPartitionRecordRetrieverProvider;
import sleeper.query.runner.recordretrieval.LeafPartitionRecordRetrieverImpl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Provides record retrievers for running Sleeper queries with Hadoop.
 */
@FunctionalInterface
public interface SleeperClientHadoopQueryProvider {

    /**
     * Creates or retrieves a record retriever provider.
     *
     * @param  hadoopConf the Hadoop configuration
     * @return            the record retriever
     */
    ShutdownWrapper<LeafPartitionRecordRetrieverProvider> getRecordRetrieverProvider(Configuration hadoopConf);

    /**
     * Creates a provider that will create a thread pool of the default size. A new thread pool will be created for each
     * Sleeper client and closed when the Sleeper client is closed.
     *
     * @return the provider
     */
    static SleeperClientHadoopQueryProvider createDefaultForEachClient() {
        return withThreadPoolForEachClient(10);
    }

    /**
     * Creates a provider that will create a new thread pool for each Sleeper client, that will be closed when the
     * Sleeper client is closed.
     *
     * @param  threadPoolSize the number of threads in the thread pool for each client
     * @return                the provider
     */
    static SleeperClientHadoopQueryProvider withThreadPoolForEachClient(int threadPoolSize) {
        return hadoopConf -> {
            ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);
            LeafPartitionRecordRetrieverProvider provider = LeafPartitionRecordRetrieverImpl.createProvider(executorService, hadoopConf);
            return ShutdownWrapper.shutdown(provider, executorService::shutdown);
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

    class PersistentThreadPool implements SleeperClientHadoopQueryProvider, UncheckedAutoCloseable {
        private final ExecutorService executorService;

        private PersistentThreadPool(ExecutorService executorService) {
            this.executorService = executorService;
        }

        @Override
        public ShutdownWrapper<LeafPartitionRecordRetrieverProvider> getRecordRetrieverProvider(Configuration hadoopConf) {
            return ShutdownWrapper.noShutdown(
                    LeafPartitionRecordRetrieverImpl.createProvider(executorService, hadoopConf));
        }

        @Override
        public void close() {
            executorService.shutdown();
        }

    }

}
