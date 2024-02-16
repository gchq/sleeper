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
            TableStatus tableId = createOnlineTable("test-table");

            assertThat(index.streamAllTables())
                    .containsExactly(tableId);
        }

        @Test
        void shouldPutTableOnlineWhenItIsCreated() {
            TableStatus tableId = createOnlineTable("test-table");

            assertThat(index.streamOnlineTables())
                    .containsExactly(tableId);
        }

        @Test
        void shouldFailToCreateATableWhichAlreadyExists() {
            createOnlineTable("duplicate-table");

            assertThatThrownBy(() -> createOnlineTable("duplicate-table"))
                    .isInstanceOf(TableAlreadyExistsException.class);
        }
    }

    @Nested
    @DisplayName("Look up a table")
    class LookupTable {

        @Test
        void shouldGetTableByName() {
            TableStatus tableId = createOnlineTable("test-table");

            assertThat(index.getTableByName("test-table"))
                    .contains(tableId);
        }

        @Test
        void shouldGetNoTableByName() {
            createOnlineTable("existing-table");

            assertThat(index.getTableByName("not-a-table"))
                    .isEmpty();
        }

        @Test
        void shouldGetTableById() {
            TableStatus tableId = createOnlineTable("test-table");

            assertThat(index.getTableByUniqueId(tableId.getTableUniqueId()))
                    .contains(tableId);
        }

        @Test
        void shouldGetNoTableById() {
            createOnlineTable("existing-table");

            assertThat(index.getTableByUniqueId("not-a-table"))
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("List tables")
    class ListTables {

        @Test
        void shouldGetTablesOrderedByName() {
            createOnlineTable("some-table");
            createOnlineTable("a-table");
            createOnlineTable("this-table");
            createOnlineTable("other-table");

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
            TableStatus table1 = createOnlineTable("first-table");
            TableStatus table2 = createOnlineTable("second-table");

            assertThat(index.streamAllTables())
                    .containsExactly(table1, table2);
        }

        @Test
        void shouldGetNoTables() {
            assertThat(index.streamAllTables()).isEmpty();
        }

        @Test
        void shouldGetOnlineTables() {
            // Given
            TableStatus table1 = createOnlineTable("online-table");
            TableStatus table2 = createOnlineTable("offline-table");
            index.update(table2.takeOffline());

            // When / Then
            assertThat(index.streamOnlineTables())
                    .containsExactly(table1);
        }
    }

    @Nested
    @DisplayName("Delete table")
    class DeleteTable {

        @Test
        void shouldDeleteTableNameReference() {
            // Given
            TableStatus tableId = createOnlineTable("test-table");

            // When
            index.delete(tableId);

            // Then
            assertThat(index.getTableByName("test-table")).isEmpty();
        }

        @Test
        void shouldDeleteTableIdReference() {
            // Given
            TableStatus tableId = createOnlineTable("test-table");

            // When
            index.delete(tableId);

            // Then
            assertThat(index.getTableByUniqueId(tableId.getTableUniqueId())).isEmpty();
        }

        @Test
        void shouldDeleteAllTablesWhileStreamingThroughIds() {
            // Given
            createOnlineTable("test-table-1");
            createOnlineTable("test-table-2");

            // When
            index.streamAllTables().forEach(index::delete);

            // Then
            assertThat(index.streamAllTables()).isEmpty();
        }

        @Test
        void shouldFailToDeleteTableWhenTableNameHasBeenUpdated() {
            // Given
            TableStatus oldTableId = createOnlineTable("old-name");
            TableStatus newTableId = TableStatus.uniqueIdAndName(oldTableId.getTableUniqueId(), "new-name");
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
            TableStatus tableId = TableStatus.uniqueIdAndName("not-a-table-id", "not-a-table");

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
            TableStatus tableId = createOnlineTable("old-name");

            // When
            TableStatus newTableId = TableStatus.uniqueIdAndName(tableId.getTableUniqueId(), "new-name");
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
            TableStatus newTableId = TableStatus.uniqueIdAndName("not-a-table-id", "new-name");

            // When/Then
            assertThatThrownBy(() -> index.update(newTableId))
                    .isInstanceOf(TableNotFoundException.class);
            assertThat(index.streamAllTables()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Take offline")
    class TakeOffline {
        @Test
        void shouldTakeTableOffline() {
            // Given
            TableStatus table = createOnlineTable("test-table");

            // When
            index.update(table.takeOffline());

            // Then
            assertThat(index.streamOnlineTables()).isEmpty();
        }

        @Test
        void shouldFailToTakeTableOfflineIfTableDoesNotExist() {
            // When / Then
            assertThatThrownBy(() -> index.update(TableStatus.uniqueIdAndName("not-a-table-id", "not-a-table").takeOffline()))
                    .isInstanceOf(TableNotFoundException.class);
        }

        @Test
        void shouldFailToTakeTableOfflineIfTableHasBeenDeleted() {
            // Given
            TableStatus table = createOnlineTable("test-table");
            index.delete(table);

            // When / Then
            assertThatThrownBy(() -> index.update(table.takeOffline()))
                    .isInstanceOf(TableNotFoundException.class);
        }
    }

    @Nested
    @DisplayName("Put table online")
    class PutOnline {
        @Test
        void shouldPutTableOnline() {
            // Given
            TableStatus onlineTable = createOnlineTable("test-table");
            TableStatus offlineTable = onlineTable.takeOffline();
            index.update(offlineTable);

            // When
            index.update(onlineTable);

            // Then
            assertThat(index.streamOnlineTables())
                    .containsExactly(onlineTable);
        }

        @Test
        void shouldFailToPutTableOnlineWhenTableDoesNotExist() {
            // When / Then
            assertThatThrownBy(() -> index.update(TableStatus.uniqueIdAndName("not-a-table-id", "not-a-table").putOnline()))
                    .isInstanceOf(TableNotFoundException.class);
        }
    }

    private TableStatus createOnlineTable(String tableName) {
        TableStatus tableId = TableStatus.uniqueIdAndName(idGenerator.generateString(), tableName, true);
        index.create(tableId);
        return tableId;
    }
}
