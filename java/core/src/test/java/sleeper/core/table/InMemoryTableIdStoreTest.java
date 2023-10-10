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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

public class InMemoryTableIdStoreTest {

    private final TableIdStore store = new InMemoryTableIdStore();

    @Nested
    @DisplayName("Create a table")
    class CreateTable {
        @Test
        void shouldCreateATable() {
            TableId tableId = store.createTable("test-table");

            assertThat(store.streamAllTables())
                    .containsExactly(tableId);
        }

        @Test
        void shouldFailToCreateATableWhichAlreadyExists() {
            store.createTable("duplicate-table");

            assertThatThrownBy(() -> store.createTable("duplicate-table"))
                    .isInstanceOf(TableAlreadyExistsException.class);
        }

        @Test
        void shouldGenerateNumericTableIds() {
            TableId tableIdA = store.createTable("table-a");
            TableId tableIdB = store.createTable("table-b");

            assertThat(List.of(tableIdA, tableIdB))
                    .extracting(TableId::getTableName, TableId::getTableId)
                    .containsExactly(
                            tuple("table-a", "table-1"),
                            tuple("table-b", "table-2"));
        }
    }

    @Nested
    @DisplayName("Look up a table")
    class LookupTable {

        @Test
        void shouldGetTableByName() {
            TableId tableId = store.createTable("test-table");

            assertThat(store.getTableByName("test-table"))
                    .contains(tableId);
        }

        @Test
        void shouldGetNoTableByName() {
            store.createTable("existing-table");

            assertThat(store.getTableByName("not-a-table"))
                    .isEmpty();
        }

        @Test
        void shouldGetTableById() {
            TableId tableId = store.createTable("test-table");

            assertThat(store.getTableById(tableId.getTableId()))
                    .contains(tableId);
        }

        @Test
        void shouldGetNoTableById() {
            store.createTable("existing-table");

            assertThat(store.getTableById("not-a-table"))
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("List tables")
    class ListTables {

        @Test
        void shouldGetTablesOrderedByName() {
            store.createTable("some-table");
            store.createTable("a-table");
            store.createTable("this-table");
            store.createTable("other-table");

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
            TableId table1 = store.createTable("first-table");
            TableId table2 = store.createTable("second-table");

            assertThat(store.streamAllTables())
                    .containsExactly(table1, table2);
        }

        @Test
        void shouldGetNoTables() {
            assertThat(store.streamAllTables()).isEmpty();
        }
    }
}
