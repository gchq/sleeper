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
package sleeper.configuration.properties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.testutils.TablePropertiesITBase;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.table.TableAlreadyExistsException;
import sleeper.core.table.TableNotFoundException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.core.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

class S3TablePropertiesStoreIT extends TablePropertiesITBase {

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
    @DisplayName("Load table properties")
    class LoadProperties {

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

        @Test
        void shouldFindNoTableById() {
            assertThatThrownBy(() -> store.loadById("not-a-table"))
                    .isInstanceOf(TableNotFoundException.class);
        }
    }
}
