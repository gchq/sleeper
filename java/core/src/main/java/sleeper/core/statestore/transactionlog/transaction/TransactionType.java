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
package sleeper.core.statestore.transactionlog.transaction;

/**
 * A marker for the type of a transaction. Used when storing a log entry for a transaction. Stabilises mapping between
 * a serialised transaction type and the class of the transaction, in case the class is renamed or moved.
 */
public enum TransactionType {

    ADD_FILES(AddFilesTransaction.class),
    ASSIGN_JOB_IDS(AssignJobIdsTransaction.class),
    CLEAR_FILES(ClearFilesTransaction.class),
    DELETE_FILES(DeleteFilesTransaction.class),
    INITIALISE_PARTITIONS(InitialisePartitionsTransaction.class),
    REPLACE_FILE_REFERENCES(ReplaceFileReferencesTransaction.class),
    SPLIT_FILE_REFERENCES(SplitFileReferencesTransaction.class),
    SPLIT_PARTITION(SplitPartitionTransaction.class);

    private final Class<? extends StateStoreTransaction<?>> type;

    TransactionType(Class<? extends StateStoreTransaction<?>> type) {
        this.type = type;
    }

    public Class<? extends StateStoreTransaction<?>> getType() {
        return type;
    }

    /**
     * Retrieves the type of a transaction.
     *
     * @param  transaction the transaction
     * @return             the type
     */
    public static TransactionType getType(StateStoreTransaction<?> transaction) {
        for (TransactionType type : values()) {
            if (type.getType().isInstance(transaction)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Transaction type not recognised: " + transaction.getClass());
    }
}
