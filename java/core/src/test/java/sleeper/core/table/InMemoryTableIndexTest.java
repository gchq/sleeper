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

    private final TableIndex store = new InMemoryTableIndex();
    private final TableIdGenerator idGenerator = new TableIdGenerator();

    @Nested
    @DisplayName("Create a table")
    class CreateTable {
        @Test
        void shouldCreateATable() {
            TableId tableId = createTable("test-table");

            assertThat(store.streamAllTables())
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

            assertThat(store.getTableByName("test-table"))
                    .contains(tableId);
        }

        @Test
        void shouldGetNoTableByName() {
            createTable("existing-table");

            assertThat(store.getTableByName("not-a-table"))
                    .isEmpty();
        }

        @Test
        void shouldGetTableById() {
            TableId tableId = createTable("test-table");

            assertThat(store.getTableByUniqueId(tableId.getTableUniqueId()))
                    .contains(tableId);
        }

        @Test
        void shouldGetNoTableById() {
            createTable("existing-table");

            assertThat(store.getTableByUniqueId("not-a-table"))
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

            assertThat(store.streamAllTables())
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

            assertThat(store.streamAllTables())
                    .containsExactly(table1, table2);
        }

        @Test
        void shouldGetNoTables() {
            assertThat(store.streamAllTables()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Delete table")
    class DeleteTable {

        @Test
        void deleteTableNameReference() {
            TableId tableId = createTable("test-table");

            store.delete(tableId);

            assertThat(store.getTableByName("test-table")).isEmpty();
        }

        @Test
        void deleteTableIdReference() {
            TableId tableId = createTable("test-table");

            store.delete(tableId);

            assertThat(store.getTableByUniqueId(tableId.getTableUniqueId())).isEmpty();
        }
    }

    @Nested
    @DisplayName("Update table")
    class UpdateTable {
        @Test
        void shouldUpdateTableName() {
            TableId tableId = createTable("old-name");

            TableId newTableId = TableId.uniqueIdAndName(tableId.getTableUniqueId(), "new-name");
            updateTable(newTableId);

            assertThat(store.streamAllTables())
                    .containsExactly(newTableId);
            assertThat(store.getTableByName("new-name"))
                    .contains(newTableId);
            assertThat(store.getTableByName("old-name")).isEmpty();
            assertThat(store.getTableByUniqueId(newTableId.getTableUniqueId()))
                    .contains(newTableId);
        }
    }

    private TableId createTable(String tableName) {
        TableId tableId = TableId.uniqueIdAndName(idGenerator.generateString(), tableName);
        store.create(tableId);
        return tableId;
    }

    private TableId updateTable(TableId tableId) {
        store.update(tableId);
        return tableId;
    }
}
