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

import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;

import java.util.Objects;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;

/**
 * A way to retrieve a transaction serialiser by the table ID.
 */
public interface TransactionSerDeProvider {
    /**
     * Gets a transaction serialiser for a Sleeper table.
     *
     * @param  tableId the table ID
     * @return         the serialiser
     */
    TransactionSerDe getByTableId(String tableId);

    /**
     * Creates a provider that creates transaction serialisers by loading the table properties.
     *
     * @param  tablePropertiesProvider the table properties provider
     * @return                         the serialiser provider
     */
    static TransactionSerDeProvider from(TablePropertiesProvider tablePropertiesProvider) {
        return tableId -> new TransactionSerDe(tablePropertiesProvider.getById(tableId).getSchema());
    }

    /**
     * Creates a provider that returns a transaction serialiser for one table.
     *
     * @param  tableProperties the table properties
     * @return                 the serialiser provider
     */
    static TransactionSerDeProvider forOneTable(TableProperties tableProperties) {
        String expectedTableId = tableProperties.get(TABLE_ID);
        TransactionSerDe serDe = new TransactionSerDe(tableProperties.getSchema());
        return tableId -> {
            if (Objects.equals(tableId, expectedTableId)) {
                return serDe;
            } else {
                throw new IllegalArgumentException("Expected table ID " + expectedTableId + ", found " + tableId);
            }
        };
    }
}
