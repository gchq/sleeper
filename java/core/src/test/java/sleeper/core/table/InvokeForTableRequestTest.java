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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class InvokeForTableRequestTest {

    private final InvokeForTableRequestSerDe serDe = new InvokeForTableRequestSerDe();

    @Test
    void shouldSendRequestForTwoTables() {
        List<String> sent = new ArrayList<>();
        InvokeForTableRequest.forTables(
                Stream.of(table("table-1"), table("table-2")),
                1, request -> sent.add(serDe.toJson(request)));
        assertThat(sent).extracting(serDe::fromJson).containsExactly(
                new InvokeForTableRequest(List.of("table-1")),
                new InvokeForTableRequest(List.of("table-2")));
    }

    @Test
    void shouldSendBatchesOf2() {
        List<String> sent = new ArrayList<>();
        InvokeForTableRequest.forTables(
                Stream.of(table("table-1"), table("table-2")),
                2, request -> sent.add(serDe.toJson(request)));
        assertThat(sent).extracting(serDe::fromJson).containsExactly(
                new InvokeForTableRequest(List.of("table-1", "table-2")));
    }

    @Test
    void shouldSendRequestForOnlyOnlineTable() {
        // Given
        TableIndex tableIndex = new InMemoryTableIndex();
        tableIndex.create(table("offline-table").takeOffline());
        tableIndex.create(table("online-table"));

        // When
        List<String> sent = new ArrayList<>();
        InvokeForTableRequest.forTablesWithOfflineEnabled(false, tableIndex,
                1, request -> sent.add(serDe.toJson(request)));

        // Then
        assertThat(sent).extracting(serDe::fromJson).containsExactly(
                new InvokeForTableRequest(List.of("online-table")));
    }

    @Test
    void shouldSendRequestForAllTablesWhenOfflineEnabled() {
        // Given
        TableIndex tableIndex = new InMemoryTableIndex();
        tableIndex.create(table("offline-table").takeOffline());
        tableIndex.create(table("online-table"));

        // When
        List<String> sent = new ArrayList<>();
        InvokeForTableRequest.forTablesWithOfflineEnabled(true, tableIndex,
                1, request -> sent.add(serDe.toJson(request)));

        // Then
        assertThat(sent).extracting(serDe::fromJson).containsExactly(
                new InvokeForTableRequest(List.of("offline-table")),
                new InvokeForTableRequest(List.of("online-table")));
    }

    private TableStatus table(String tableName) {
        return TableStatusTestHelper.uniqueIdAndName(tableName, tableName);
    }
}
