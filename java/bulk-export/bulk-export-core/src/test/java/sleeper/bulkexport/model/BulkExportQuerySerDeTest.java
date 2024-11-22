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
                .exportId("e1")
                .build();

        // When
        String serialisedQuery = querySerDe.toJson(bulkExportQuery);

        BulkExportQuery deserialisedQuery = querySerDe.fromJson(serialisedQuery);

        // Then
        assertThat(bulkExportQuery).isEqualTo(deserialisedQuery);
    }

    @Test
    public void shouldThrowExceptionWithNullTableNameAndTableId() {
        // When / Then
        assertThatThrownBy(() -> querySerDe.toJson(BulkExportQuery.builder()
                .exportId("id")
                .build()))
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage("Query validation failed for export \"id\": " +
                        "tableId or tableName field must be provided");
    }

    @Test
    public void shouldThrowExceptionNoTableNameAndNoTableId() {
        // Given
        String queryJson = "{\n" +
                "  \"exportId\": \"id\"\n" +
                "}\n";

        // When & Then
        assertThatThrownBy(() -> querySerDe.fromJson(queryJson))
                .isInstanceOf(BulkExportQueryValidationException.class)
                .hasMessage("Query validation failed for export \"id\": " +
                        "tableId or tableName field must be provided");
    }

    @Test
    public void shouldThrowExceptionBothTableNameAndTableId() {
        // Given
        String queryJson = "{\n" +
                "  \"exportId\": \"id\"\n," +
                "  \"tableId\": \"table-id\"\n," +
                "  \"tableName\": \"id\"\n" +
                "}\n";

        // When & Then
        assertThatThrownBy(() -> querySerDe.fromJson(queryJson))
                .isInstanceOf(BulkExportQueryValidationException.class)
                .hasMessage("Query validation failed for export \"id\": " +
                        "tableId or tableName field must be provided, not both");
    }
}
