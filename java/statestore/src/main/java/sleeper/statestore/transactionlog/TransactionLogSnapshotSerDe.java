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
import sleeper.core.statestore.transactionlog.StateStoreFiles;
import sleeper.core.statestore.transactionlog.StateStorePartitions;
import sleeper.statestore.StateStoreParquetSerDe;

import java.io.IOException;

/**
 * Reads and writes snapshots derived from a transaction log to/from Parquet files.
 */
class TransactionLogSnapshotSerDe {
    private final Schema sleeperSchema;
    private final StateStoreParquetSerDe parquetSerDe;

    TransactionLogSnapshotSerDe(Schema sleeperSchema, Configuration configuration) {
        this.sleeperSchema = sleeperSchema;
        this.parquetSerDe = new StateStoreParquetSerDe(configuration);
    }

    void savePartitions(TransactionLogSnapshotMetadata snapshot, StateStorePartitions state) throws IOException {
        parquetSerDe.savePartitions(snapshot.getPath(), state, sleeperSchema);
    }

    StateStorePartitions loadPartitions(TransactionLogSnapshotMetadata snapshot) throws IOException {
        StateStorePartitions partitions = new StateStorePartitions();
        parquetSerDe.loadPartitions(snapshot.getPath(), sleeperSchema, partitions::put);
        return partitions;
    }

    void saveFiles(TransactionLogSnapshotMetadata snapshot, StateStoreFiles state) throws IOException {
        parquetSerDe.saveFiles(snapshot.getPath(), state);
    }

    StateStoreFiles loadFiles(TransactionLogSnapshotMetadata snapshot) throws IOException {
        StateStoreFiles files = new StateStoreFiles();
        parquetSerDe.loadFiles(snapshot.getPath(), files::add);
        return files;
    }
}
