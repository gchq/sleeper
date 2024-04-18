/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.statestore.transactionlog;

import org.apache.hadoop.conf.Configuration;

import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.StateStoreFiles;
import sleeper.core.statestore.transactionlog.StateStorePartitions;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;

public class TransactionLogSnapshot {
    private final TransactionLogPartitionsSnapshot partitionsSnapshot;
    private final TransactionLogFilesSnapshot filesSnapshot;
    private final TransactionLogStateStore store;

    public static TransactionLogSnapshot from(Schema schema, TransactionLogStateStore store, Configuration configuration) {
        return new TransactionLogSnapshot(schema, store, configuration);
    }

    TransactionLogSnapshot(Schema schema, TransactionLogStateStore store, Configuration configuration) {
        this.partitionsSnapshot = new TransactionLogPartitionsSnapshot(schema, store, configuration);
        this.filesSnapshot = new TransactionLogFilesSnapshot(configuration);
        this.store = store;
    }

    void savePartitions(java.nio.file.Path tempDir, long lastTransactionNumber) throws StateStoreException {
        partitionsSnapshot.save(tempDir, lastTransactionNumber);
    }

    StateStorePartitions loadPartitions(java.nio.file.Path tempDir, long lastTransactionNumber) throws StateStoreException {
        return partitionsSnapshot.load(tempDir, lastTransactionNumber);
    }

    void saveFiles(java.nio.file.Path tempDir, StateStoreFiles state, long lastTransactionNumber) throws StateStoreException {
        filesSnapshot.save(tempDir, state, lastTransactionNumber);
    }

    StateStoreFiles loadFiles(java.nio.file.Path tempDir, long lastTransactionNumber) throws StateStoreException {
        return filesSnapshot.load(tempDir, lastTransactionNumber);
    }
}
