/*
 * Copyright 2022-2023 Crown Copyright
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
            TableId tableId = createTable("test-table");

            assertThat(index.streamAllTables())
                    .containsExactly(tableId);
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
            TableId tableId = createTable("test-table");

            assertThat(index.getTableByName("test-table"))
                    .contains(tableId);
        }

        @Test
        void shouldGetNoTableByName() {
            createTable("existing-table");

            assertThat(index.getTableByName("not-a-table"))
                    .isEmpty();
        }

        @Test
        void shouldGetTableById() {
            TableId tableId = createTable("test-table");

            assertThat(index.getTableByUniqueId(tableId.getTableUniqueId()))
                    .contains(tableId);
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
                    .extracting(TableId::getTableName)
                    .containsExactly(
                            "a-table",
                            "other-table",
                            "some-table",
                            "this-table");
        }

        @Test
        void shouldGetTableIds() {
            TableId table1 = createTable("first-table");
            TableId table2 = createTable("second-table");

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
            TableId tableId = createTable("test-table");

            // When
            index.delete(tableId);

            // Then
            assertThat(index.getTableByName("test-table")).isEmpty();
        }

        @Test
        void shouldDeleteTableIdReference() {
            // Given
            TableId tableId = createTable("test-table");

            // When
            index.delete(tableId);

            // Then
            assertThat(index.getTableByUniqueId(tableId.getTableUniqueId())).isEmpty();
        }

        @Test
        void shouldFailToDeleteTableWhenTableNameHasBeenUpdated() {
            // Given
            TableId oldTableId = createTable("old-name");
            TableId newTableId = TableId.uniqueIdAndName(oldTableId.getTableUniqueId(), "new-name");
            index.update(newTableId);

            // When / Then
            assertThatThrownBy(() -> index.delete(oldTableId))
                    .isInstanceOf(TableNotFoundException.class);
            assertThat(index.streamAllTables()).contains(newTableId);
            assertThat(index.getTableByName("old-name")).isEmpty();
            assertThat(index.getTableByName("new-name")).contains(newTableId);
        }

        @Test
        void shouldFailToDeleteTableThatDoesNotExist() {
            // Given
            TableId tableId = TableId.uniqueIdAndName("not-a-table-id", "not-a-table");

            // When / Then
            assertThatThrownBy(() -> index.delete(tableId))
                    .isInstanceOf(TableNotFoundException.class);
        }
    }

    @Nested
    @DisplayName("Update table")
    class UpdateTable {
        @Test
        void shouldUpdateTableName() {
            // Given
            TableId tableId = createTable("old-name");

            // When
            TableId newTableId = TableId.uniqueIdAndName(tableId.getTableUniqueId(), "new-name");
            index.update(newTableId);

            // Then
            assertThat(index.streamAllTables())
                    .containsExactly(newTableId);
            assertThat(index.getTableByName("new-name"))
                    .contains(newTableId);
            assertThat(index.getTableByName("old-name")).isEmpty();
            assertThat(index.getTableByUniqueId(newTableId.getTableUniqueId()))
                    .contains(newTableId);
        }

        @Test
        void shouldFailToUpdateTableIfTableDoesNotExist() {
            // Given
            TableId newTableId = TableId.uniqueIdAndName("not-a-table-id", "new-name");

            // When/Then
            assertThatThrownBy(() -> index.update(newTableId))
                    .isInstanceOf(TableNotFoundException.class);
            assertThat(index.streamAllTables()).isEmpty();
        }
    }

    private TableId createTable(String tableName) {
        TableId tableId = TableId.uniqueIdAndName(idGenerator.generateString(), tableName);
        index.create(tableId);
        return tableId;
    }
}
