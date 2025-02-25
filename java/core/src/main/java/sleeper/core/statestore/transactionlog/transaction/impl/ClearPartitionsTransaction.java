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

import java.util.List;

/**
 * A transaction to clear partitions in a Sleeper table.
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
