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
package sleeper.bulkexport.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class BulkExportQuerySerDeTest {

    private final BulkExportQuerySerDe querySerDe = new BulkExportQuerySerDe();

    @Test
    public void shouldSerDeBulkExportQuery() {
        // Given
        BulkExportQuery bulkExportQuery = BulkExportQuery.builder()
                .tableName("test-table")
                .tableId("t1")
                .exportId("e1")
                .build();

        // When
        String serialisedQuery = querySerDe.toJson(bulkExportQuery);

        BulkExportQuery deserialisedQuery = querySerDe.fromJson(serialisedQuery);

        // Then
        assertThat(bulkExportQuery).isEqualTo(deserialisedQuery);
    }

    @Test
    public void shouldThrowExceptionWithNullTableName() {
        // When / Then
        assertThatThrownBy(() -> querySerDe.toJson(BulkExportQuery.builder()
                .exportId("id")
                .tableId("id")
                .build()))
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage("Query validation failed for export \"id\": " +
                        "tableName field must be provided");
    }

    @Test
    public void shouldThrowExceptionNoExportId() {
        String queryJson = "{\n" +
                "  \"tableName\": \"test-table\",\n" +
                "  \"tableId\": \"id2\"\n" +
                "}\n";

        // When & Then
        assertThatThrownBy(() -> querySerDe.fromJson(queryJson))
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage("Query validation failed: exportId field must be provided");
    }

    @Test
    public void shouldThrowExceptionNoTableName() {
        // Given
        String queryJson = "{\n" +
                "  \"exportId\": \"id\",\n" +
                "  \"tableId\": \"id2\"\n" +
                "}\n";

        // When & Then
        assertThatThrownBy(() -> querySerDe.fromJson(queryJson))
                .isInstanceOf(BulkExportQueryValidationException.class)
                .hasMessage("Query validation failed for export \"id\": " +
                        "tableName field must be provided");
    }
}
