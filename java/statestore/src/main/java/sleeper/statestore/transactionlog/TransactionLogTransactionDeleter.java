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

import sleeper.core.statestore.transactionlog.TransactionLogStore;

/**
 * Given a state store, finds transactions that are old enough to be deleted and deletes them.
 */
public class TransactionLogTransactionDeleter {
    private final TransactionLogStore filesLogStore;
    private final TransactionLogStore partitionsLogStore;
    private final GetLatestSnapshots getLatestSnapshots;

    public TransactionLogTransactionDeleter(
            TransactionLogStore filesLogStore, TransactionLogStore partitionsLogStore,
            GetLatestSnapshots getLatestSnapshots) {
        this.filesLogStore = filesLogStore;
        this.partitionsLogStore = partitionsLogStore;
        this.getLatestSnapshots = getLatestSnapshots;
    }

    /**
     * Finds transactions that are old enough to be deleted and deletes them.
     */
    public void delete() {
    }

    /**
     * Retrieves metadata of the latest snapshots.
     */
    @FunctionalInterface
    public interface GetLatestSnapshots {
        /**
         * Retrieves the latest snapshots metadata.
         *
         * @return the metadata
         */
        LatestSnapshots getLatestSnapshots();
    }

}
