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
package sleeper.systemtest.dsl.testutil;

import sleeper.core.row.testutils.InMemoryRowStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.ingest.batcher.core.testutil.InMemoryIngestBatcherStore;
import sleeper.sketches.testutils.InMemorySketchesStore;
import sleeper.systemtest.dsl.instance.SleeperInstanceDriver;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryBulkExport;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryCompaction;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryIngestByQueue;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryReports;
import sleeper.systemtest.dsl.testutil.drivers.InMemorySleeperInstanceDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemorySleeperTablesDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryStateStoreCommitter;
import sleeper.systemtest.dsl.testutil.drivers.InMemorySystemTestDeploymentDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryTableMetrics;

public class InMemorySystemTestState {

    private final InMemoryRowStore sourceFiles = new InMemoryRowStore();
    private final InMemoryRowStore data = new InMemoryRowStore();
    private final InMemorySketchesStore sketches = new InMemorySketchesStore();
    private final InMemoryTransactionLogsPerTable transactionLogs = new InMemoryTransactionLogsPerTable();
    private final InMemoryIngestBatcherStore batcherStore = new InMemoryIngestBatcherStore();
    private final SystemTestDeploymentDriver systemTestDeploymentDriver = new InMemorySystemTestDeploymentDriver();
    private final InMemorySleeperTablesDriver tablesDriver = new InMemorySleeperTablesDriver(transactionLogs);
    private final SleeperInstanceDriver instanceDriver = new InMemorySleeperInstanceDriver(tablesDriver);
    private final InMemoryIngestByQueue ingestByQueue = new InMemoryIngestByQueue(sourceFiles, data, sketches);
    private final InMemoryCompaction compaction = new InMemoryCompaction(data, sketches);
    private final InMemoryTableMetrics metrics = new InMemoryTableMetrics();
    private final InMemoryReports reports = new InMemoryReports(ingestByQueue, compaction);

    private long fileSizeBytesForBatcher = 1024;

    public void setFileSizeBytesForBatcher(long fileSizeBytesForBatcher) {
        this.fileSizeBytesForBatcher = fileSizeBytesForBatcher;
    }

    public long getFileSizeBytesForBatcher() {
        return fileSizeBytesForBatcher;
    }

    public InMemoryRowStore getSourceFiles() {
        return sourceFiles;
    }

    public InMemoryRowStore getData() {
        return data;
    }

    public InMemorySketchesStore getSketches() {
        return sketches;
    }

    public InMemoryTransactionLogsPerTable getTransactionLogs() {
        return transactionLogs;
    }

    public InMemoryIngestBatcherStore getBatcherStore() {
        return batcherStore;
    }

    public SystemTestDeploymentDriver getSystemTestDeploymentDriver() {
        return systemTestDeploymentDriver;
    }

    public InMemorySleeperTablesDriver getTablesDriver() {
        return tablesDriver;
    }

    public SleeperInstanceDriver getInstanceDriver() {
        return instanceDriver;
    }

    public InMemoryIngestByQueue getIngestByQueue() {
        return ingestByQueue;
    }

    public InMemoryCompaction getCompaction() {
        return compaction;
    }

    public InMemoryTableMetrics getMetrics() {
        return metrics;
    }

    public InMemoryReports getReports() {
        return reports;
    }

    public InMemoryBulkExport getBulkExport() {
        return bulkExport;
    }

    public InMemoryStateStoreCommitter getStateStoreCommitter() {
        return stateStoreCommitter;
    }

    private final InMemoryBulkExport bulkExport = new InMemoryBulkExport();
    private final InMemoryStateStoreCommitter stateStoreCommitter = new InMemoryStateStoreCommitter(transactionLogs.getTransactionBodyStore(), ingestByQueue, compaction);

}
