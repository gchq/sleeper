/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.bulkexport.core.model;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class BulkExportQueryTest {

    @Test
    public void testEqualsAndHashcode() {
        // Given
        String tableId = UUID.randomUUID().toString();
        String exportId = UUID.randomUUID().toString();
        BulkExportQuery query1 = BulkExportQuery.builder()
                .tableId(tableId)
                .exportId(exportId)
                .sqlQuery("SELECT * FROM query_results;")
                .build();
        BulkExportQuery query2 = BulkExportQuery.builder()
                .tableId(tableId)
                .exportId(exportId)
                .sqlQuery("SELECT * FROM query_results;")
                .build();
        BulkExportQuery query3 = BulkExportQuery.builder()
                .tableId(tableId)
                .exportId(exportId.split("-")[0])
                .sqlQuery("SELECT * FROM query_results;")
                .build();

        // When / Then
        assertThat(query1).isEqualTo(query2);
        assertThat(query1).isNotEqualTo(query3);
        assertThat(query1.hashCode()).isEqualTo(query2.hashCode());
        assertThat(query1.hashCode()).isNotEqualTo(query3.hashCode());
    }

    @Test
    public void testAutoUUIDForExportId() {
        // Given When
        BulkExportQuery query1 = BulkExportQuery.builder()
                .tableId("id")
                .build();
        // Then
        assertThat(query1.getExportId()).isNotNull();
    }

    @Test
    public void testTableIdAndNameCannotBothBeSet() {
        // Given When Then
        assertThatThrownBy(() -> BulkExportQuery.builder()
                .exportId("960b3b01")
                .tableId("id")
                .tableName("test")
                .build())
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage(
                        "Query validation failed for export \"960b3b01\": tableId or tableName field must be provided, not both");
    }

    @Test
    public void testTableIdAndNameBothMissing() {
        // Given When Then
        assertThatThrownBy(() -> BulkExportQuery.builder()
                .exportId("960b3b01")
                .build())
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage(
                        "Query validation failed for export \"960b3b01\": tableId or tableName field must be provided");
    }
}
