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
package sleeper.configuration.statestore;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.InMemoryTransactionLogs;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

/**
 * Gathers state for multiple state stores backed by in-memory transaction logs. Helps with independent management of
 * the local state of the state store, by creating separate state store objects.
 */
public class InMemoryTransactionLogsPerTable {

    private final Map<String, InMemoryTransactionLogs> transactionLogsByTableId = new LinkedHashMap<>();
    private final List<Duration> retryWaits = new ArrayList<>();

    /**
     * Creates a builder for a state store backed by in-memory transaction logs.
     *
     * @param  tableProperties the Sleeper table properties
     * @return                 the builder
     */
    public TransactionLogStateStore.Builder stateStoreBuilder(TableProperties tableProperties) {
        return forTable(tableProperties)
                .stateStoreBuilder(tableProperties.getStatus(), tableProperties.getSchema());
    }

    /**
     * Creates a helper to work with a state store backed by in-memory transaction logs.
     *
     * @param  tableProperties the Sleeper table properties
     * @return                 the helper
     */
    public InMemoryTransactionLogs forTable(TableProperties tableProperties) {
        return forTableId(tableProperties.get(TABLE_ID));
    }

    /**
     * Creates a helper to work with a state store backed by in-memory transaction logs.
     *
     * @param  tableId the Sleeper table unique ID
     * @return         the helper
     */
    public InMemoryTransactionLogs forTableId(String tableId) {
        return transactionLogsByTableId.computeIfAbsent(tableId, id -> InMemoryTransactionLogs.recordRetryWaits(retryWaits));
    }

    public List<Duration> getRetryWaits() {
        return retryWaits;
    }
}
