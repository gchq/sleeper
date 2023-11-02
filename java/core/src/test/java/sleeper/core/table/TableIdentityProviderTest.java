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

import static org.assertj.core.api.Assertions.assertThat;

public class TableIdentityProviderTest {
    private final TableIndex tableIndex = new InMemoryTableIndex();
    private final TableIdentityProvider tableIdentityProvider = new TableIdentityProvider(tableIndex);

    @Test
    void shouldCacheTableIdentityById() {
        // Given
        TableIdentity before = TableIdentity.uniqueIdAndName("test-table-id", "test-table");
        tableIndex.create(before);
        tableIdentityProvider.getById("test-table-id");

        // When
        TableIdentity after = TableIdentity.uniqueIdAndName("test-table-id", "new-table-name");
        tableIndex.update(after);

        // Then
        assertThat(tableIdentityProvider.getById("test-table-id"))
                .contains(before);
    }

    @Test
    void shouldReportTableDoesNotExist() {
        // When / Then
        assertThat(tableIdentityProvider.getById("not-a-table-id"))
                .isEmpty();
    }

    @Test
    void shouldCacheThatTableDoesNotExist() {
        // Given
        tableIdentityProvider.getById("table-id");

        // When
        tableIndex.create(TableIdentity.uniqueIdAndName("table-id", "table-name"));

        // When / Then
        assertThat(tableIdentityProvider.getById("table-id"))
                .isEmpty();
    }
}
