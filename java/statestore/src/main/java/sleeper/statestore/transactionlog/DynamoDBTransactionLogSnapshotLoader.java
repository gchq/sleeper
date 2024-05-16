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

import sleeper.core.statestore.transactionlog.TransactionLogSnapshot;
import sleeper.core.statestore.transactionlog.TransactionLogSnapshotLoader;

import java.util.Optional;

public class DynamoDBTransactionLogSnapshotLoader implements TransactionLogSnapshotLoader {

    private final DynamoDBTransactionLogSnapshotStore snapshotStore;
    private final TransactionLogSnapshotSerDe snapshotSerDe;

    public DynamoDBTransactionLogSnapshotLoader(
            DynamoDBTransactionLogSnapshotStore snapshotStore, TransactionLogSnapshotSerDe snapshotSerDe) {
        this.snapshotStore = snapshotStore;
        this.snapshotSerDe = snapshotSerDe;
    }

    @Override
    public Optional<TransactionLogSnapshot> loadLatestSnapshotIfAtMinimumTransaction(long transactionNumber) {
        snapshotStore.getLatestSnapshots();
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'loadLatestSnapshotIfAtMinimumTransaction'");
    }

}
