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
package sleeper.statestore.transactionlog.snapshots;

import sleeper.core.statestore.transactionlog.log.TransactionLogRange;
import sleeper.core.statestore.transactionlog.snapshot.TransactionLogSnapshot;
import sleeper.core.statestore.transactionlog.snapshot.TransactionLogSnapshotLoader;
import sleeper.statestore.StateStoreArrowFileReadStore;

import java.util.Optional;

/**
 * Loads the latest snapshot of state derived from a transaction log, where the snapshot is held in S3 and tracked in
 * DynamoDB.
 */
public class DynamoDBTransactionLogSnapshotLoader implements TransactionLogSnapshotLoader {

    private final DynamoDBTransactionLogSnapshotMetadataStore metadataStore;
    private final StateStoreArrowFileReadStore fileStore;
    private final SnapshotType snapshotType;

    public DynamoDBTransactionLogSnapshotLoader(
            DynamoDBTransactionLogSnapshotMetadataStore metadataStore, StateStoreArrowFileReadStore fileStore, SnapshotType snapshotType) {
        this.metadataStore = metadataStore;
        this.fileStore = fileStore;
        this.snapshotType = snapshotType;
    }

    @Override
    public Optional<TransactionLogSnapshot> loadLatestSnapshotInRange(TransactionLogRange range) {
        return metadataStore.getLatestSnapshotInRange(snapshotType, range)
                .map(fileStore::loadSnapshot);
    }

}
