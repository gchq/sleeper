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

package sleeper.core.properties.testutils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.table.TableAlreadyExistsException;
import sleeper.core.table.TableNotFoundException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.core.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.core.properties.table.TableProperty.SCHEMA;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;

public class InMemoryTablePropertiesStoreTest {
    private static final Schema KEY_VALUE_SCHEMA = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .valueFields(new Field("value", new StringType()))
            .build();

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createValidTableProperties();
    private final TablePropertiesStore store = InMemoryTableProperties.getStore();
    private final String tableName = tableProperties.get(TABLE_NAME);
    private final String tableId = tableProperties.get(TABLE_ID);

    @Nested
    @DisplayName("Save table properties")
    class SaveProperties {

        @Test
        void shouldCreateNewTable() {
            // When
            store.createTable(tableProperties);

            // Then
            assertThat(store.loadByName(tableName))
                    .isEqualTo(tableProperties);
        }

        @Test
        void shouldNotCreateDuplicateTable() {
            // Given
            store.createTable(tableProperties);

            // When / Then
            assertThatThrownBy(() -> store.createTable(tableProperties))
                    .isInstanceOf(TableAlreadyExistsException.class);
        }

        @Test
        void shouldCreateNewTableWithSave() {
            // When
            store.save(tableProperties);

            // Then
            assertThat(store.loadByName(tableName))
                    .isEqualTo(tableProperties);
        }

        @Test
        void shouldUpdateTableProperties() {
            // Given
            tableProperties.setNumber(PAGE_SIZE, 123);
            store.save(tableProperties);
            tableProperties.setNumber(PAGE_SIZE, 456);
            store.save(tableProperties);

            // When / Then
            assertThat(store.loadByName(tableName))
                    .extracting(properties -> properties.getInt(PAGE_SIZE))
                    .isEqualTo(456);
        }

        @Test
        void shouldUpdateTableName() {
            // Given
            store.save(tableProperties);
            tableProperties.set(TABLE_NAME, "renamed-table");
            store.save(tableProperties);

            // When / Then
            assertThat(store.loadByName("renamed-table"))
                    .extracting(properties -> properties.get(TABLE_NAME))
                    .isEqualTo("renamed-table");
        }

        @Test
        void shouldNotUpdateTableNameIfNewNameIsTheSameAsExistingTable() {
            // Given
            tableProperties.set(TABLE_NAME, "old-name");
            store.save(tableProperties);
            TableProperties table2 = createValidTableProperties();
            table2.set(TABLE_NAME, "new-name");
            store.save(table2);

            // When / Then
            tableProperties.set(TABLE_NAME, "new-name");
            assertThatThrownBy(() -> store.save(tableProperties))
                    .isInstanceOf(TableAlreadyExistsException.class);
            assertThat(store.loadById(tableId))
                    .extracting(table -> table.get(TABLE_NAME))
                    .isEqualTo("old-name");
        }
    }

    @Nested
    @DisplayName("Update table properties")
    class UpdateProperties {

        @Test
        void shouldThrowExceptionIfTableDoesNotExists() {
            // When / Then
            assertThatThrownBy(() -> store.update(tableProperties))
                    .isInstanceOf(TableNotFoundException.class);
        }

        @Test
        void shouldUpdateTableProperties() {
            // Given
            tableProperties.setNumber(PAGE_SIZE, 123);
            store.createTable(tableProperties);
            tableProperties.setNumber(PAGE_SIZE, 456);

            //When
            store.update(tableProperties);

            // When / Then
            assertThat(store.loadByName(tableName))
                    .extracting(properties -> properties.getInt(PAGE_SIZE))
                    .isEqualTo(456);
        }
    }

    @Nested
    @DisplayName("Delete properties")
    class DeleteProperties {
        @Test
        void shouldDeleteATable() {
            // Given
            store.save(tableProperties);

            // When
            store.deleteByName(tableName);

            // Then
            assertThatThrownBy(() -> store.loadByName(tableName))
                    .isInstanceOf(TableNotFoundException.class);
            assertThatThrownBy(() -> store.loadById(tableId))
                    .isInstanceOf(TableNotFoundException.class);
        }
    }

    @Nested
    @DisplayName("Load properties by ID")
    class LoadById {

        @Test
        void shouldLoadTableById() {
            // When
            store.save(tableProperties);

            // Then
            assertThat(store.loadById(tableId))
                    .isEqualTo(tableProperties);
        }

        @Test
        void shouldNotLoadInvalidTableById() {
            // When
            tableProperties.set(COMPRESSION_CODEC, "abc");
            store.save(tableProperties);

            // Then
            assertThatThrownBy(() -> store.loadById(tableId))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void shouldFindNoTableById() {
            assertThatThrownBy(() -> store.loadById("not-a-table"))
                    .isInstanceOf(TableNotFoundException.class);
        }
    }

    @Nested
    @DisplayName("Load properties by name")
    class LoadByName {

        @Test
        void shouldNotLoadInvalidTableByName() {
            // When
            tableProperties.set(COMPRESSION_CODEC, "abc");
            store.save(tableProperties);

            // Then
            assertThatThrownBy(() -> store.loadByName(tableProperties.get(TABLE_NAME)))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void shouldLoadInvalidTablePropertiesByName() {
            // When
            tableProperties.set(COMPRESSION_CODEC, "abc");
            store.save(tableProperties);

            // Then
            assertThat(store.loadByNameNoValidation(tableProperties.get(TABLE_NAME)))
                    .extracting(properties -> properties.get(COMPRESSION_CODEC))
                    .isEqualTo("abc");
        }

        @Test
        void shouldFindNoTableByName() {
            assertThatThrownBy(() -> store.loadByName("not-a-table"))
                    .isInstanceOf(TableNotFoundException.class);
        }

        @Test
        void shouldFindNoTableByNameNoValidation() {
            assertThatThrownBy(() -> store.loadByNameNoValidation("not-a-table"))
                    .isInstanceOf(TableNotFoundException.class);
        }
    }

    @Nested
    @DisplayName("Load properties of all tables")
    class LoadAll {

        @Test
        void shouldLoadOnlineAndOfflineTables() {
            // Given
            TableProperties table1 = createValidTableProperties();
            TableProperties table2 = createValidTableProperties();
            table2.set(TABLE_ONLINE, "false");
            store.save(table1);
            store.save(table2);

            // When / Then
            assertThat(store.streamAllTables())
                    .containsExactly(table1, table2);
        }

        @Test
        void shouldIncludeTableWithInvalidProperty() {
            // Given
            tableProperties.set(SCHEMA, "{}");
            store.save(tableProperties);

            // When / Then
            assertThat(store.streamAllTables())
                    .containsExactly(tableProperties);
        }
    }

    @Nested
    @DisplayName("Load properties of online tables")
    class LoadOnline {

        @Test
        void shouldLoadOnlyOnlineTables() {
            // Given
            TableProperties table1 = createValidTableProperties();
            TableProperties table2 = createValidTableProperties();
            TableProperties table3 = createValidTableProperties();
            table2.set(TABLE_ONLINE, "false");
            store.save(table1);
            store.save(table2);
            store.save(table3);

            // When / Then
            assertThat(store.streamOnlineTables())
                    .containsExactly(table1, table3);
        }

        @Test
        void shouldIncludeTableWithInvalidProperty() {
            // Given
            tableProperties.set(SCHEMA, "{}");
            store.save(tableProperties);

            // When / Then
            assertThat(store.streamOnlineTables())
                    .containsExactly(tableProperties);
        }
    }

    protected TableProperties createValidTableProperties() {
        return createTestTableProperties(instanceProperties, KEY_VALUE_SCHEMA);
    }
}
