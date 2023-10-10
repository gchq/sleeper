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
        String tableId = store.createTableGetId("test-table");

        assertThat(store.streamAllTables())
                .containsExactly(TableId.idAndName(tableId, "test-table"));
    }

    @Test
    void shouldFailToCreateATableWhichAlreadyExists() {
        store.createTableGetId("duplicate-table");
        assertThatThrownBy(() -> store.createTableGetId("duplicate-table"))
                .isInstanceOf(TableAlreadyExistsException.class);
    }

    @Test
    void shouldGenerateNumericTableIds() {
        String tableIdA = store.createTableGetId("A");
        String tableIdB = store.createTableGetId("B");

        assertThat(List.of(tableIdA, tableIdB))
                .containsExactly("table-1", "table-2");
    }
}
