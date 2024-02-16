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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TableStatusProviderTest {
    private final TableIndex tableIndex = new InMemoryTableIndex();
    private final TableStatusProvider tableStatusProvider = new TableStatusProvider(tableIndex);

    @Test
    void shouldCacheTableStatusById() {
        // Given
        TableStatus before = TableStatus.uniqueIdAndName("test-table-id", "test-table");
        tableIndex.create(before);
        tableStatusProvider.getById("test-table-id");

        // When
        TableStatus after = TableStatus.uniqueIdAndName("test-table-id", "new-table-name");
        tableIndex.update(after);

        // Then
        assertThat(tableStatusProvider.getById("test-table-id"))
                .contains(before);
    }

    @Test
    void shouldReportTableDoesNotExist() {
        // When / Then
        assertThat(tableStatusProvider.getById("not-a-table-id"))
                .isEmpty();
    }

    @Test
    void shouldCacheThatTableDoesNotExist() {
        // Given
        tableStatusProvider.getById("table-id");

        // When
        tableIndex.create(TableStatus.uniqueIdAndName("table-id", "table-name"));

        // When / Then
        assertThat(tableStatusProvider.getById("table-id"))
                .isEmpty();
    }
}
