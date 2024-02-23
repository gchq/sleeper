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
            TableStatus table = createOnlineTable("test-table");

            assertThat(index.streamAllTables())
                    .containsExactly(table);
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
            TableStatus table = createOnlineTable("test-table");

            assertThat(index.getTableByName("test-table"))
                    .contains(table);
        }

        @Test
        void shouldGetNoTableByName() {
            createOnlineTable("existing-table");

            assertThat(index.getTableByName("not-a-table"))
                    .isEmpty();
        }

        @Test
        void shouldGetTableById() {
            TableStatus table = createOnlineTable("test-table");

            assertThat(index.getTableByUniqueId(table.getTableUniqueId()))
                    .contains(table);
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
            TableStatus table = createOnlineTable("test-table");

            // When
            index.delete(table);

            // Then
            assertThat(index.getTableByName("test-table")).isEmpty();
        }

        @Test
        void shouldDeleteTableIdReference() {
            // Given
            TableStatus table = createOnlineTable("test-table");

            // When
            index.delete(table);

            // Then
            assertThat(index.getTableByUniqueId(table.getTableUniqueId())).isEmpty();
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
            TableStatus oldTable = createOnlineTable("old-name");
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

        @Test
        void shouldFailToDeleteTableIfTableRenamedAfterLoadingOldId() {
            // Given
            TableStatus old = TableStatus.uniqueIdAndName("test-id", "old-name");
            TableStatus renamed = TableStatus.uniqueIdAndName("test-id", "changed-name");
            index.create(old);
            index.update(renamed);

            // When/Then
            assertThatThrownBy(() -> index.delete(old))
                    .isInstanceOf(TableNotFoundException.class);
            assertThat(index.streamAllTables()).contains(renamed);
        }

        @Test
        void shouldFailToDeleteTableIfTableDeletedAndRecreatedAfterLoadingOldId() {
            // Given
            TableStatus old = TableStatus.uniqueIdAndName("test-id-1", "table-name");
            TableStatus recreated = TableStatus.uniqueIdAndName("test-id-2", "table-name");
            index.create(recreated);

            // When/Then
            assertThatThrownBy(() -> index.delete(old))
                    .isInstanceOf(TableNotFoundException.class);
        }
    }

    @Nested
    @DisplayName("Update table")
    class UpdateTable {
        @Test
        void shouldUpdateTableName() {
            // Given
            TableStatus table = createOnlineTable("old-name");

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

        @Test
        void shouldFailToUpdateTableIfTableWithSameNameAlreadyExists() {
            // Given
            createOnlineTable("test-name-1");
            TableStatus table2 = createOnlineTable("test-name-2");

            // When / Then
            TableStatus newTable = TableStatus.uniqueIdAndName(table2.getTableUniqueId(), "test-name-1");
            assertThatThrownBy(() -> index.update(newTable))
                    .isInstanceOf(TableAlreadyExistsException.class);
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
        TableStatus table = TableStatus.uniqueIdAndName(idGenerator.generateString(), tableName, true);
        index.create(table);
        return table;
    }
}
