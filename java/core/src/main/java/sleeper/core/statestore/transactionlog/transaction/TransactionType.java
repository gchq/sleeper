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
package sleeper.core.statestore.transactionlog.transaction;

import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.AssignJobIdsTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.ClearFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.DeleteFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.ExtendPartitionTreeTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.InitialisePartitionsTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.SplitFileReferencesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.SplitPartitionTransaction;

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
    SPLIT_PARTITION(SplitPartitionTransaction.class),
    EXTEND_PARTITION_TREE(ExtendPartitionTreeTransaction.class);

    private final Class<? extends StateStoreTransaction<?>> type;
    private final boolean fileTransaction;

    TransactionType(Class<? extends StateStoreTransaction<?>> type) {
        this.type = type;
        this.fileTransaction = FileReferenceTransaction.class.isAssignableFrom(type);
    }

    public Class<? extends StateStoreTransaction<?>> getType() {
        return type;
    }

    public boolean isFileTransaction() {
        return fileTransaction;
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

    /**
     * Retrieves the type of a transaction class.
     *
     * @param  transactionClass the transaction class
     * @return                  the type
     */
    public static TransactionType getType(Class<? extends StateStoreTransaction<?>> transactionClass) {
        for (TransactionType type : values()) {
            if (type.getType().isAssignableFrom(transactionClass)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Transaction type not recognised: " + transactionClass);
    }
}
