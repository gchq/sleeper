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

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class BulkExportLeafPartitionQuerySerDeTest {

    private final BulkExportLeafPartitionQuerySerDe querySerDe = new BulkExportLeafPartitionQuerySerDe();

    @Test
    public void shouldSerDeBulkExportLeafPartitionQuery() {
        // Given
        BulkExportLeafPartitionQuery bulkExportLeafPartitionQuery = BulkExportLeafPartitionQuery.builder()
                .tableId("t1")
                .exportId("e1")
                .subExportId("se1")
                .leafPartitionId("lp1")
                .files(Collections.singletonList("/test/file.parquet"))
                .build();

        // When
        String serialisedQuery = querySerDe.toJson(bulkExportLeafPartitionQuery);

        BulkExportLeafPartitionQuery deserialisedQuery = querySerDe.fromJson(serialisedQuery);

        // Then
        assertThat(bulkExportLeafPartitionQuery).isEqualTo(deserialisedQuery);
    }

    @Test
    public void shouldThrowExceptionWithNullTableId() {
        // When / Then
        assertThatThrownBy(() -> querySerDe.toJson(BulkExportLeafPartitionQuery.builder()
                .exportId("id")
                .subExportId("se1")
                .leafPartitionId("lp1")
                .files(Collections.singletonList("/test/file.parquet"))
                .build()))
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage("Query validation failed for export \"id\": " +
                        "tableId field must be provided");
    }

    @Test
    public void shouldThrowExceptionNoExportId() {
        String queryJson = "{\n" +
                "  \"tableId\": \"id2\",\n" +
                "  \"subExportId\": \"se1\",\n" +
                "  \"leafPartitionId\": \"p1\",\n" +
                "  \"files\": []\n" +
                "}\n";

        // When & Then
        assertThatThrownBy(() -> querySerDe.fromJson(queryJson))
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage("Query validation failed: exportId field must be provided");
    }

    @Test
    public void shouldThrowExceptionNoSubExportId() {
        String queryJson = "{\n" +
                "  \"tableId\": \"id2\",\n" +
                "  \"exportId\": \"id\",\n" +
                "  \"leafPartitionId\": \"p1\",\n" +
                "  \"files\": []\n" +
                "}\n";

        // When & Then
        assertThatThrownBy(() -> querySerDe.fromJson(queryJson))
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage("Query validation failed for export \"id\": subExportId field must be provided");
    }

    @Test
    public void shouldThrowExceptionNoLeafPartitionId() {
        String queryJson = "{\n" +
                "  \"tableId\": \"id2\",\n" +
                "  \"exportId\": \"id\",\n" +
                "  \"subExportId\": \"se1\",\n" +
                "  \"files\": []\n" +
                "}\n";

        // When & Then
        assertThatThrownBy(() -> querySerDe.fromJson(queryJson))
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage("Query validation failed for export \"id\": leafPartitionId field must be provided");
    }

    @Test
    public void shouldThrowExceptionNoFiles() {
        String queryJson = "{\n" +
                "  \"tableId\": \"id2\",\n" +
                "  \"exportId\": \"id\",\n" +
                "  \"subExportId\": \"se1\",\n" +
                "  \"leafPartitionId\": \"p1\"\n" +
                "}\n";

        // When & Then
        assertThatThrownBy(() -> querySerDe.fromJson(queryJson))
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage("Query validation failed for export \"id\": files field must be provided");
    }
}
