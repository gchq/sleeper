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
package sleeper.statestore.transactionlog.snapshots;

import org.apache.hadoop.conf.Configuration;

import sleeper.core.schema.Schema;
import sleeper.core.statestore.transactionlog.StateStoreFiles;
import sleeper.core.statestore.transactionlog.StateStorePartitions;
import sleeper.statestore.StateStoreArrowFileStore;

import java.io.IOException;

/**
 * Reads and writes snapshots derived from a transaction log to/from Parquet files.
 */
class TransactionLogSnapshotSerDe {
    private final Schema sleeperSchema;
    private final StateStoreArrowFileStore dataStore;

    TransactionLogSnapshotSerDe(Schema sleeperSchema, Configuration configuration) {
        this.sleeperSchema = sleeperSchema;
        this.dataStore = new StateStoreArrowFileStore(configuration);
    }

    void savePartitions(TransactionLogSnapshotMetadata snapshot, StateStorePartitions state) throws IOException {
        dataStore.savePartitions(snapshot.getPath(), state.all(), sleeperSchema);
    }

    StateStorePartitions loadPartitions(TransactionLogSnapshotMetadata snapshot) throws IOException {
        StateStorePartitions partitions = new StateStorePartitions();
        dataStore.loadPartitions(snapshot.getPath(), sleeperSchema).forEach(partitions::put);
        return partitions;
    }

    void saveFiles(TransactionLogSnapshotMetadata snapshot, StateStoreFiles state) throws IOException {
        dataStore.saveFiles(snapshot.getPath(), state);
    }

    StateStoreFiles loadFiles(TransactionLogSnapshotMetadata snapshot) throws IOException {
        return dataStore.loadFiles(snapshot.getPath());
    }
}
