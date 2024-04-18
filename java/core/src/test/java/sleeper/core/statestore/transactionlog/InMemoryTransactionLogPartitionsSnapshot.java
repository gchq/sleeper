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
package sleeper.core.statestore.transactionlog;

import sleeper.core.statestore.StateStoreException;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class InMemoryTransactionLogPartitionsSnapshot implements TransactionLogPartitionsSnapshot {
    private final List<String> filenames = new ArrayList<>();
    private final TransactionLogStore logStore;
    private final TransactionLogHeadLoader<StateStorePartitions> headLoader;

    public static TransactionLogPartitionsSnapshot from(
            TransactionLogStore logStore, TransactionLogHeadLoader<StateStorePartitions> headLoader) {
        return new InMemoryTransactionLogPartitionsSnapshot(logStore, headLoader);
    }

    InMemoryTransactionLogPartitionsSnapshot(TransactionLogStore logStore, TransactionLogHeadLoader<StateStorePartitions> headLoader) {
        this.logStore = logStore;
        this.headLoader = headLoader;
    }

    public StateStorePartitions state() throws StateStoreException {
        return headLoader.getState(logStore);
    }

    public long lastTransactionNumber() throws StateStoreException {
        return headLoader.getLastTransactionNumber(logStore);
    }

    public void save(Path tempDir) throws StateStoreException {
        TransactionLogHead<StateStorePartitions> head = headLoader.head(logStore);
        head.update();
        filenames.add(tempDir.resolve(createPath(head)).toString());
    }

    List<String> getSavedFiles() {
        return filenames;
    }
}
