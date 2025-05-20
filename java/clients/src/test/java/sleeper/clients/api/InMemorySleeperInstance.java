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
package sleeper.clients.api;

import sleeper.bulkimport.core.configuration.BulkImportPlatform;
import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.testutils.InMemoryTableProperties;
import sleeper.core.record.testutils.InMemoryRecordStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequest;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.runner.testutils.InMemoryIngest;
import sleeper.ingest.runner.testutils.InMemorySketchesStore;
import sleeper.query.core.recordretrieval.InMemoryLeafPartitionRecordRetriever;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class InMemorySleeperInstance {

    private final InstanceProperties properties;
    private final InMemoryTableIndex tableIndex = new InMemoryTableIndex();
    private final TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStoreReturningExactInstance(tableIndex);
    private final TablePropertiesProvider tablePropertiesProvider;
    private final InMemoryTransactionLogsPerTable transactionLogsPerTable = new InMemoryTransactionLogsPerTable();
    private final StateStoreProvider stateStoreProvider;
    private final InMemoryRecordStore dataStore = new InMemoryRecordStore();
    private final InMemorySketchesStore sketchesStore = new InMemorySketchesStore();
    private final Queue<IngestJob> ingestQueue = new LinkedList<>();
    private final Map<BulkImportPlatform, Queue<BulkImportJob>> bulkImportQueues = new HashMap<>();
    private final Queue<IngestBatcherSubmitRequest> ingestBatcherQueue = new LinkedList<>();

    public InMemorySleeperInstance(InstanceProperties properties) {
        this.properties = properties;
        this.tablePropertiesProvider = new TablePropertiesProvider(properties, tablePropertiesStore);
        this.stateStoreProvider = InMemoryTransactionLogStateStore.createProvider(properties, transactionLogsPerTable);
    }

    public SleeperClient.Builder sleeperClientBuilder() {
        return new SleeperClient.Builder()
                .instanceProperties(properties)
                .tableIndex(tableIndex)
                .tablePropertiesStore(tablePropertiesStore)
                .tablePropertiesProvider(tablePropertiesProvider)
                .stateStoreProvider(stateStoreProvider)
                .recordRetrieverProvider(new InMemoryLeafPartitionRecordRetriever(dataStore))
                .ingestJobSender(ingestQueue::add)
                .bulkImportJobSender(bulkImportSender())
                .ingestBatcherSender(ingestBatcherQueue::add);
    }

    public InMemoryIngest ingestByTableName(String tableName) {
        return ingest(tablePropertiesProvider.getByName(tableName));
    }

    public InMemoryIngest ingest(TableProperties tableProperties) {
        return new InMemoryIngest(properties,
                tableProperties,
                stateStoreProvider.getStateStore(tableProperties),
                dataStore, sketchesStore);
    }

    public Queue<BulkImportJob> bulkImportQueue(BulkImportPlatform platform) {
        return bulkImportQueues.computeIfAbsent(platform, p -> new LinkedList<>());
    }

    public InstanceProperties properties() {
        return properties;
    }

    public InMemoryTableIndex tableIndex() {
        return tableIndex;
    }

    public TablePropertiesStore tablePropertiesStore() {
        return tablePropertiesStore;
    }

    public TablePropertiesProvider tablePropertiesProvider() {
        return tablePropertiesProvider;
    }

    public InMemoryTransactionLogsPerTable transactionLogsPerTable() {
        return transactionLogsPerTable;
    }

    public StateStoreProvider stateStoreProvider() {
        return stateStoreProvider;
    }

    public InMemoryRecordStore dataStore() {
        return dataStore;
    }

    public InMemorySketchesStore sketchesStore() {
        return sketchesStore;
    }

    public Queue<IngestJob> ingestQueue() {
        return ingestQueue;
    }

    public Map<BulkImportPlatform, Queue<BulkImportJob>> bulkImportQueues() {
        return bulkImportQueues;
    }

    public Queue<IngestBatcherSubmitRequest> ingestBatcherQueue() {
        return ingestBatcherQueue;
    }

    private BulkImportJobSender bulkImportSender() {
        return (platform, job) -> bulkImportQueue(platform).add(job);
    }

}
