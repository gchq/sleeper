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

public class InMemorySystemTestState {

    private final InMemoryRowStore sourceFiles = new InMemoryRowStore();
    private final InMemoryRowStore data = new InMemoryRowStore();
    private final InMemorySketchesStore sketches = new InMemorySketchesStore();
    private final InMemoryTransactionLogsPerTable transactionLogs = new InMemoryTransactionLogsPerTable();
    private final InMemoryIngestBatcherStore batcherStore = new InMemoryIngestBatcherStore();

    private long fileSizeBytesForBatcher = 1024;

    public InMemoryRowStore data() {
        return data;
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

    public long getFileSizeBytesForBatcher() {
        return fileSizeBytesForBatcher;
    }

    public void setFileSizeBytesForBatcher(long sizeForBatcher) {
        this.fileSizeBytesForBatcher = sizeForBatcher;
    }

}
