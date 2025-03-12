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
package sleeper.core.statestore.transactionlog.transaction.impl;

import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.transaction.FileReferenceTransaction;

import java.time.Instant;
import java.util.Objects;

/**
 * Clears all file data from the file reference store. Note that this does not delete any of the actual files.
 */
public class ClearFilesTransaction implements FileReferenceTransaction {

    @Override
    public void validate(StateStoreFiles stateStoreFiles) throws StateStoreException {
    }

    /**
     * Commit this transaction directly to the state store without going to the commit queue. This will throw any
     * validation exceptions immediately, even if they wouldn't be as part of an asynchronous commit.
     *
     * @param  stateStore          the state store
     * @throws StateStoreException if the update fails
     */
    public void synchronousCommit(StateStore stateStore) throws StateStoreException {
        stateStore.addFilesTransaction(AddTransactionRequest.withTransaction(this).build());
    }

    @Override
    public void apply(StateStoreFiles stateStoreFiles, Instant updateTime) {
        stateStoreFiles.clear();
    }

    @Override
    public int hashCode() {
        return Objects.hash();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ClearFilesTransaction;
    }

    @Override
    public String toString() {
        return "ClearFilesTransaction{}";
    }
}
