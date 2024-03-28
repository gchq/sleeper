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

public interface FileReferenceTransaction extends StateStoreTransaction {
    @Override
    default void validate(TransactionLogHead state) throws StateStoreException {
        validate(state.files());
    }

    void validate(StateStoreFiles stateStoreFiles) throws StateStoreException;

    @Override
    default void apply(TransactionLogHead state) {
        apply(state.files());
    }

    void apply(StateStoreFiles stateStoreFiles);
}
