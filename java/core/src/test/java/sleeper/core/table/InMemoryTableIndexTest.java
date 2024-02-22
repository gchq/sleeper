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

package sleeper.core.table;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class InMemoryTableIndexTest {

    private final TableIndex index = new InMemoryTableIndex();
    private final TableIdGenerator idGenerator = new TableIdGenerator();

    @Nested
    @DisplayName("Create a table")
    class CreateTable {
        @Test
        void shouldCreateATable() {
            TableStatus table = createTable("test-table");

            assertThat(index.streamAllTables())
                    .containsExactly(table);
        }

        @Test
        void shouldFailToCreateATableWhichAlreadyExists() {
            createTable("duplicate-table");

            assertThatThrownBy(() -> createTable("duplicate-table"))
                    .isInstanceOf(TableAlreadyExistsException.class);
        }
    }

    @Nested
    @DisplayName("Look up a table")
    class LookupTable {

        @Test
        void shouldGetTableByName() {
            TableStatus table = createTable("test-table");

            assertThat(index.getTableByName("test-table"))
                    .contains(table);
        }

        @Test
        void shouldGetNoTableByName() {
            createTable("existing-table");

            assertThat(index.getTableByName("not-a-table"))
                    .isEmpty();
        }

        @Test
        void shouldGetTableById() {
            TableStatus table = createTable("test-table");

            assertThat(index.getTableByUniqueId(table.getTableUniqueId()))
                    .contains(table);
        }

        @Test
        void shouldGetNoTableById() {
            createTable("existing-table");

            assertThat(index.getTableByUniqueId("not-a-table"))
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("List tables")
    class ListTables {

        @Test
        void shouldGetTablesOrderedByName() {
            createTable("some-table");
            createTable("a-table");
            createTable("this-table");
            createTable("other-table");

            assertThat(index.streamAllTables())
                    .extracting(TableStatus::getTableName)
                    .containsExactly(
                            "a-table",
                            "other-table",
                            "some-table",
                            "this-table");
        }

        @Test
        void shouldGetTableIds() {
            TableStatus table1 = createTable("first-table");
            TableStatus table2 = createTable("second-table");

            assertThat(index.streamAllTables())
                    .containsExactly(table1, table2);
        }

        @Test
        void shouldGetNoTables() {
            assertThat(index.streamAllTables()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Delete table")
    class DeleteTable {

        @Test
        void shouldDeleteTableNameReference() {
            // Given
            TableStatus table = createTable("test-table");

            // When
            index.delete(table);

            // Then
            assertThat(index.getTableByName("test-table")).isEmpty();
        }

        @Test
        void shouldDeleteTableIdReference() {
            // Given
            TableStatus table = createTable("test-table");

            // When
            index.delete(table);

            // Then
            assertThat(index.getTableByUniqueId(table.getTableUniqueId())).isEmpty();
        }

        @Test
        void shouldDeleteAllTablesWhileStreamingThroughIds() {
            // Given
            createTable("test-table-1");
            createTable("test-table-2");

            // When
            index.streamAllTables().forEach(index::delete);

            // Then
            assertThat(index.streamAllTables()).isEmpty();
        }

        @Test
        void shouldFailToDeleteTableWhenTableNameHasBeenUpdated() {
            // Given
            TableStatus oldTable = createTable("old-name");
            TableStatus newTable = TableStatus.uniqueIdAndName(oldTable.getTableUniqueId(), "new-name");
            index.update(newTable);

            // When / Then
            assertThatThrownBy(() -> index.delete(oldTable))
                    .isInstanceOf(TableNotFoundException.class);
            assertThat(index.streamAllTables()).contains(newTable);
            assertThat(index.getTableByName("old-name")).isEmpty();
            assertThat(index.getTableByName("new-name")).contains(newTable);
        }

        @Test
        void shouldFailToDeleteTableThatDoesNotExist() {
            // Given
            TableStatus table = TableStatus.uniqueIdAndName("not-a-table-id", "not-a-table");

            // When / Then
            assertThatThrownBy(() -> index.delete(table))
                    .isInstanceOf(TableNotFoundException.class);
        }
    }

    @Nested
    @DisplayName("Update table")
    class UpdateTable {
        @Test
        void shouldUpdateTableName() {
            // Given
            TableStatus table = createTable("old-name");

            // When
            TableStatus newTable = TableStatus.uniqueIdAndName(table.getTableUniqueId(), "new-name");
            index.update(newTable);

            // Then
            assertThat(index.streamAllTables())
                    .containsExactly(newTable);
            assertThat(index.getTableByName("new-name"))
                    .contains(newTable);
            assertThat(index.getTableByName("old-name")).isEmpty();
            assertThat(index.getTableByUniqueId(newTable.getTableUniqueId()))
                    .contains(newTable);
        }

        @Test
        void shouldFailToUpdateTableIfTableDoesNotExist() {
            // Given
            TableStatus newTable = TableStatus.uniqueIdAndName("not-a-table-id", "new-name");

            // When/Then
            assertThatThrownBy(() -> index.update(newTable))
                    .isInstanceOf(TableNotFoundException.class);
            assertThat(index.streamAllTables()).isEmpty();
        }
    }

    private TableStatus createTable(String tableName) {
        TableStatus table = TableStatus.uniqueIdAndName(idGenerator.generateString(), tableName);
        index.create(table);
        return table;
    }
}
