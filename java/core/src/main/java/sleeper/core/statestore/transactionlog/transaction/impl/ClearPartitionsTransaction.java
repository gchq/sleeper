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
package sleeper.core.statestore.transactionlog.transaction.impl;

import java.util.List;

/**
 * Clears all partition data from the store. Note that this will invalidate any file references held in the store,
 * so this should only be used when no files are present. The store must be initialised before the Sleeper table can
 * be used again. Any file references will need to be added again.
 */
public class ClearPartitionsTransaction {

    private ClearPartitionsTransaction() {
    }

    /**
     * Creates a transaction to clear partitions in the table.
     *
     * @return the transaction
     */
    public static InitialisePartitionsTransaction create() {
        return new InitialisePartitionsTransaction(List.of());
    }

}
