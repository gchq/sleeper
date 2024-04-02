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
package sleeper.core.statestore.transactionlog.transactions;

import sleeper.core.statestore.transactionlog.StateStoreTransaction;

public enum TransactionType {

    ADD_FILES(AddFilesTransaction.class),
    ASSIGN_JOB_IDS(AssignJobIdsTransaction.class),
    CLEAR_FILES(ClearFilesTransaction.class),
    DELETE_FILES(DeleteFilesTransaction.class),
    INITIALISE_PARTITIONS(InitialisePartitionsTransaction.class),
    REPLACE_FILE_REFERENCES(ReplaceFileReferencesTransaction.class),
    SPLIT_FILE_REFERENCES(SplitFileReferencesTransaction.class),
    SPLIT_PARTITION(SplitPartitionTransaction.class);

    private final Class<? extends StateStoreTransaction> type;

    TransactionType(Class<? extends StateStoreTransaction> type) {
        this.type = type;
    }

    public Class<? extends StateStoreTransaction> getType() {
        return type;
    }

    public static TransactionType getType(StateStoreTransaction transaction) {
        for (TransactionType type : values()) {
            if (type.getType().isInstance(transaction)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Transaction type not recognised: " + transaction.getClass());
    }
}
