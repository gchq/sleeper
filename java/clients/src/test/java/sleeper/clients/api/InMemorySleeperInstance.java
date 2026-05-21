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
package sleeper.clients.api;

import sleeper.bulkexport.core.model.BulkExportQuery;
import sleeper.bulkimport.core.configuration.BulkImportPlatform;
import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.clients.query.FakeWebSocketConnection;
import sleeper.clients.query.QueryWebSocketClient;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.testutils.InMemoryTableProperties;
import sleeper.core.row.testutils.InMemoryRowStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequest;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.runner.testutils.InMemoryIngest;
import sleeper.query.core.rowretrieval.InMemoryLeafPartitionRowRetriever;
import sleeper.sketches.testutils.InMemorySketchesStore;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * An in-memory implementation of Sleeper. Used to create a {@link SleeperClient} for unit testing.
 */
public class InMemorySleeperInstance {

    private final InstanceProperties properties;
    private final InMemoryTableIndex tableIndex = new InMemoryTableIndex();
    private final TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStoreReturningExactInstance(tableIndex);
    private final TablePropertiesProvider tablePropertiesProvider;
    private final InMemoryTransactionLogsPerTable transactionLogsPerTable = new InMemoryTransactionLogsPerTable();
    private final StateStoreProvider stateStoreProvider;
    private final InMemoryRowStore dataStore = new InMemoryRowStore();
    private final InMemorySketchesStore sketchesStore = new InMemorySketchesStore();
    private final Queue<IngestJob> ingestQueue = new LinkedList<>();
    private final Map<BulkImportPlatform, Queue<BulkImportJob>> bulkImportQueues = new HashMap<>();
    private final Queue<IngestBatcherSubmitRequest> ingestBatcherQueue = new LinkedList<>();
    private final Queue<BulkExportQuery> bulkExportQueue = new LinkedList<>();
    private final FakeWebSocketConnection webSocketConnection;

    public InMemorySleeperInstance(InstanceProperties properties) {
        this.properties = properties;
        this.tablePropertiesProvider = new TablePropertiesProvider(properties, tablePropertiesStore);
        this.stateStoreProvider = InMemoryTransactionLogStateStore.createProvider(properties, transactionLogsPerTable);
        this.webSocketConnection = new FakeWebSocketConnection();
    }

    public FakeWebSocketConnection getFakeWebSocketConnection() {
        return webSocketConnection;
    }

    /**
     * Creates a builder for a Sleeper client that will interact with this instance.
     *
     * @return the builder
     */
    public SleeperClient.Builder sleeperClientBuilder() {
        return new SleeperClient.Builder()
                .instanceProperties(properties)
                .tableIndex(tableIndex)
                .tablePropertiesStore(tablePropertiesStore)
                .tablePropertiesProvider(tablePropertiesProvider)
                .stateStoreProvider(stateStoreProvider)
                .rowRetrieverProvider(new InMemoryLeafPartitionRowRetriever(dataStore))
                .ingestJobSender(ingestQueue::add)
                .bulkImportJobSender(bulkImportSender())
                .ingestBatcherSender(ingestBatcherQueue::add)
                .bulkExportQuerySender(bulkExportQueue::add)
                .queryWebSocketSender(queryWebSocketSender());
    }

    /**
     * Prepares for ingest to a Sleeper table.
     *
     * @param  tableName the name of the table
     * @return           a factory for an ingest coordinator
     */
    public InMemoryIngest ingestByTableName(String tableName) {
        return ingest(tablePropertiesProvider.getByName(tableName));
    }

    /**
     * Prepares for ingest to a Sleeper table.
     *
     * @param  tableProperties the table properties
     * @return                 a factory for an ingest coordinator
     */
    public InMemoryIngest ingest(TableProperties tableProperties) {
        return new InMemoryIngest(properties,
                tableProperties,
                stateStoreProvider.getStateStore(tableProperties),
                dataStore, sketchesStore);
    }

    /**
     * Retrieves an in-memory queue of bulk import jobs. Each platform has its own queue of jobs to be run.
     *
     * @param  platform the platform to retrieve the queue for
     * @return          the queue
     */
    public Queue<BulkImportJob> bulkImportQueue(BulkImportPlatform platform) {
        return bulkImportQueues.computeIfAbsent(platform, p -> new LinkedList<>());
    }

    /**
     * Retrieves the instance properties. The returned object is the only copy of the properties, so any edits will
     * apply to the whole instance immediately.
     *
     * @return the instance properties
     */
    public InstanceProperties properties() {
        return properties;
    }

    /**
     * Retrieves the table index. This tracks what tables are in the instance, and their status. Updates must be applied
     * via {@link #tablePropertiesStore()}, in order for changes to be reflected in the table properties.
     *
     * @return the table index
     */
    public InMemoryTableIndex tableIndex() {
        return tableIndex;
    }

    /**
     * Retrieves the table properties store. Table properties objects returned from the store will be the only copy of
     * the properties, so any edits will apply to the whole instance immediately without being saved back to the store.
     * Saving the table properties back to the store will apply any edits to the table index when necessary.
     *
     * @return the table properties store
     */
    public TablePropertiesStore tablePropertiesStore() {
        return tablePropertiesStore;
    }

    /**
     * Retrieves the table properties provider. Table properties objects returned from the provider will be the only
     * copy of the properties, so any edits will apply to the whole instance immediately, unless you edit a property
     * that needs to be applied to the table index. To apply changes to the table index, use
     * {@link #tablePropertiesStore()}.
     *
     * @return the table properties provider
     */
    public TablePropertiesProvider tablePropertiesProvider() {
        return tablePropertiesProvider;
    }

    /**
     * Retrieves the transaction logs held in memory. These are the backing for the Sleeper table state, which you can
     * interact with through the state store via {@link #stateStoreProvider()}.
     *
     * @return the transaction logs
     */
    public InMemoryTransactionLogsPerTable transactionLogsPerTable() {
        return transactionLogsPerTable;
    }

    /**
     * Retrieves the state store provider. This is used to interact with the Sleeper table state, which is backed by the
     * transaction logs returned from {@link #transactionLogsPerTable()}.
     *
     * @return the state store provider
     */
    public StateStoreProvider stateStoreProvider() {
        return stateStoreProvider;
    }

    /**
     * Retrieves the in-memory store of table data. This contains the contents of simulated files held in Sleeper
     * tables.
     *
     * @return the store
     */
    public InMemoryRowStore dataStore() {
        return dataStore;
    }

    /**
     * Retrieves the in-memory store of sketches data. This contains data sketches of simulated files held in Sleeper
     * tables.
     *
     * @return the store
     */
    public InMemorySketchesStore sketchesStore() {
        return sketchesStore;
    }

    /**
     * Retrieves the in-memory queue of ingest jobs.
     *
     * @return the queue
     */
    public Queue<IngestJob> ingestQueue() {
        return ingestQueue;
    }

    /**
     * Retrieves the map of in-memory queues of bulk import jobs. A queue will be added for a platform automatically
     * when bulk import jobs are sent for that platform.
     *
     * @return the queues
     */
    public Map<BulkImportPlatform, Queue<BulkImportJob>> bulkImportQueues() {
        return bulkImportQueues;
    }

    /**
     * Retrieves the in-memory queue of submissions to the ingest batcher.
     *
     * @return the queue
     */
    public Queue<IngestBatcherSubmitRequest> ingestBatcherQueue() {
        return ingestBatcherQueue;
    }

    private BulkImportJobSender bulkImportSender() {
        return (platform, job) -> bulkImportQueue(platform).add(job);
    }

    private QueryWebSocketSender queryWebSocketSender() {
        return query -> {
            QueryWebSocketClient client = new QueryWebSocketClient(properties, tablePropertiesProvider, webSocketConnection.createAdapter(), 0);
            return client.submitQuery(query);
        };
    }

    /**
     * Retrieves the in-memory queue of bulk export queries.
     *
     * @return the queue
     */
    public Queue<BulkExportQuery> bulkExportQueue() {
        return bulkExportQueue;
    }

}
