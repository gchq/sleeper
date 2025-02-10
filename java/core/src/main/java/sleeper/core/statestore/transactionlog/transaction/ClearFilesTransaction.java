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

import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;

import java.time.Instant;
import java.util.Objects;

/**
 * A transaction to delete all files in the state store.
 */
public class ClearFilesTransaction implements FileReferenceTransaction {

    @Override
    public void validate(StateStoreFiles stateStoreFiles) throws StateStoreException {
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
