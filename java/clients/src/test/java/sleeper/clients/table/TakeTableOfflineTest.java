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

package sleeper.clients.table;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.testutils.InMemoryTableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.table.TableStatus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.SCHEMA;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.table.TableStatusTestHelper.uniqueIdAndName;

public class TakeTableOfflineTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = createSchemaWithKey("key1");
    private final TablePropertiesStore propertiesStore = InMemoryTableProperties.getStore();

    @Test
    void shouldTakeTableOffline() throws Exception {
        // Given
        createTable(uniqueIdAndName("test-table-1", "table-1"));

        // When
        takeTableOffline("table-1");

        // Then
        assertThat(propertiesStore.loadByName("table-1").getBoolean(TABLE_ONLINE)).isFalse();
    }

    @Test
    void shouldTakeTableOfflineDespiteInvalidSchema() throws Exception {
        // Given
        TableProperties table = createTable(uniqueIdAndName("test-table-1", "table-1"));
        table.set(SCHEMA, "{}");
        propertiesStore.update(table);

        // When
        takeTableOffline("table-1");

        // Then
        assertThat(propertiesStore.loadByNameNoValidation("table-1").getBoolean(TABLE_ONLINE)).isFalse();
    }

    @Test
    void shouldFailToDeleteTableThatDoesNotExist() {
        // When / Then
        assertThatThrownBy(() -> takeTableOffline("table-1"))
                .isInstanceOf(TableNotFoundException.class);
    }

    private void takeTableOffline(String tableName) throws Exception {
        new TakeTableOffline(propertiesStore).takeOffline(tableName);
    }

    private TableProperties createTable(TableStatus tableStatus) {
        TableProperties table = createTestTableProperties(instanceProperties, schema);
        table.set(TABLE_ID, tableStatus.getTableUniqueId());
        table.set(TABLE_NAME, tableStatus.getTableName());
        propertiesStore.save(table);
        return table;
    }
}
