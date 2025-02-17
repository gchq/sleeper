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

import sleeper.core.properties.table.TableProperties;

/**
 * Provides state store for in memory transaction logs.
 */
public class InMemoryTransactionLogStateStore {

    private InMemoryTransactionLogStateStore() {
    }

    /**
     * Creates a state store backed by transaction logs held in memory.
     *
     * @param  tableProperties the table properties
     * @param  transactionLogs the transaction logs
     * @return                 the state store
     */
    public static TransactionLogStateStore create(TableProperties tableProperties, InMemoryTransactionLogs transactionLogs) {
        return transactionLogs.stateStoreBuilder(tableProperties.getStatus(), tableProperties.getSchema())
                .build();
    }
}
