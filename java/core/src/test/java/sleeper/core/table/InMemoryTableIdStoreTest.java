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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class InMemoryTableIdStoreTest {

    private final TableIdStore store = new InMemoryTableIdStore();

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
        TableId tableIdA = store.createTable("A");
        TableId tableIdB = store.createTable("B");

        assertThat(List.of(tableIdA, tableIdB)).containsExactly(
                TableId.idAndName("table-1", "A"),
                TableId.idAndName("table-2", "B"));
    }

    @Test
    void shouldGetTableByName() {
        TableId tableId = store.createTable("test-table");

        assertThat(store.getTableByName("test-table"))
                .isEqualTo(tableId);
    }

    @Test
    void shouldGetTableById() {
        TableId tableId = store.createTable("test-table");

        assertThat(store.getTableById(tableId.getTableId()))
                .isEqualTo(tableId);
    }
}
