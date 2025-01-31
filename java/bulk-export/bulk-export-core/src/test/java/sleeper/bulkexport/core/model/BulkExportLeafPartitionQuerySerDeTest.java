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
package sleeper.bulkexport.core.model;

import com.google.common.io.CharStreams;
import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;

public class BulkExportLeafPartitionQuerySerDeTest {
    private final Field field = new Field("key", new IntType());
    private final Schema schema = Schema.builder()
            .rowKeyFields(field)
            .valueFields(new Field("value1", new StringType()), new Field("value2", new StringType()))
            .build();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);

    @Test
    public void shouldSerDeBulkExportLeafPartitionQuery() {
        // Given
        String tableId = "t-id";
        tableProperties.set(TABLE_ID, tableId);

        String exportId = "e-id";
        String subExportId = "se-id";
        String leafPartitionId = "lp-id";
        RangeFactory rangeFactory = new RangeFactory(schema);
        Region region1 = new Region(rangeFactory.createRange(field, 1, true, 10, true));
        Region partitionRegion = new Region(rangeFactory.createRange(field, 0, 1000));
        BulkExportLeafPartitionQuery bulkExportLeafPartitionQuery = BulkExportLeafPartitionQuery.builder()
                .tableId(tableId)
                .exportId(exportId)
                .subExportId(subExportId)
                .regions(List.of(region1))
                .leafPartitionId(leafPartitionId)
                .partitionRegion(partitionRegion)
                .files(Collections.singletonList("/test/file.parquet"))
                .build();

        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDeFromTableProperties(schema);

        // When
        String json = querySerDe.toJson(bulkExportLeafPartitionQuery, true);
        BulkExportLeafPartitionQuery deserialisedQuery = querySerDe.fromJson(json);

        // Then
        assertThat(bulkExportLeafPartitionQuery).isEqualTo(deserialisedQuery);
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
    }

    @Test
    public void shouldSerDeBulkExportLeafPartitionQueryUsingSchema() {
        // Given
        String tableId = "t-id";
        tableProperties.set(TABLE_ID, tableId);

        String exportId = "e-id";
        String subExportId = "se-id";
        String leafPartitionId = "lp-id";
        RangeFactory rangeFactory = new RangeFactory(schema);
        Region region1 = new Region(rangeFactory.createRange(field, 1, true, 10, true));
        Region partitionRegion = new Region(rangeFactory.createRange(field, 0, 1000));
        BulkExportLeafPartitionQuery bulkExportLeafPartitionQuery = BulkExportLeafPartitionQuery.builder()
                .tableId(tableId)
                .exportId(exportId)
                .subExportId(subExportId)
                .regions(List.of(region1))
                .leafPartitionId(leafPartitionId)
                .partitionRegion(partitionRegion)
                .files(Collections.singletonList("/test/file.parquet"))
                .build();

        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDeFromSchema(schema);

        // When
        String json = querySerDe.toJson(bulkExportLeafPartitionQuery, true);
        BulkExportLeafPartitionQuery deserialisedQuery = querySerDe.fromJson(json);

        // Then
        assertThat(bulkExportLeafPartitionQuery).isEqualTo(deserialisedQuery);
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
    }

    @Test
    public void shouldThrowExceptionWithNullTableId() {
        // Given
        String tableId = "t-id";
        tableProperties.set(TABLE_ID, tableId);

        String exportId = "e-id";
        String subExportId = "se-id";
        String leafPartitionId = "lp-id";
        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDeFromTableProperties(schema);

        // When / Then
        assertThatThrownBy(() -> querySerDe.toJson(BulkExportLeafPartitionQuery.builder()
                .exportId(exportId)
                .subExportId(subExportId)
                .leafPartitionId(leafPartitionId)
                .files(Collections.singletonList("/test/file.parquet"))
                .build()))
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage("Query validation failed for export \"" + exportId + "\": " +
                        "tableId field must be provided");
    }

    @Test
    public void shouldThrowExceptionNoExportId() throws IOException {
        // Given
        String tableId = "t-id";
        tableProperties.set(TABLE_ID, tableId);
        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDeFromTableProperties(schema);

        String json = readFileToString(
                "BulkExportLeafPartitionQuerySerDeTest.shouldThrowExceptionNoExportId.json");

        // When & Then
        assertThatThrownBy(() -> querySerDe.fromJson(json))
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage("Query validation failed: exportId field must be provided");
    }

    @Test
    public void shouldThrowExceptionNoSubExportId() throws IOException {
        // Given
        String tableId = "t-id";
        tableProperties.set(TABLE_ID, tableId);
        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDeFromTableProperties(schema);

        String json = readFileToString(
                "BulkExportLeafPartitionQuerySerDeTest.shouldThrowExceptionNoSubExportId.json");

        // When & Then
        assertThatThrownBy(() -> querySerDe.fromJson(
                json))
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage(
                        "Query validation failed for export \"e-id\": subExportId field must be provided");
    }

    @Test
    public void shouldThrowExceptionNoRegions() throws IOException {
        // Given
        String tableId = "t-id";
        tableProperties.set(TABLE_ID, tableId);
        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDeFromTableProperties(schema);

        String json = readFileToString(
                "BulkExportLeafPartitionQuerySerDeTest.shouldThrowExceptionNoRegions.json");

        // When & Then
        assertThatThrownBy(() -> querySerDe.fromJson(json))
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage("Query validation failed for export \"e-id\": regions field must be provided");
    }

    @Test
    public void shouldThrowExceptionNoLeafPartitionId() throws IOException {
        // Given
        String tableId = "t-id";
        tableProperties.set(TABLE_ID, tableId);
        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDeFromTableProperties(schema);

        String json = readFileToString(
                "BulkExportLeafPartitionQuerySerDeTest.shouldThrowExceptionNoLeafPartitionId.json");

        // When & Then
        assertThatThrownBy(() -> querySerDe.fromJson(json))
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage("Query validation failed for export \"e-id\": leafPartitionId field must be provided");
    }

    @Test
    public void shouldThrowExceptionNoPartitionRegion() throws IOException {
        // Given
        String tableId = "t-id";
        tableProperties.set(TABLE_ID, tableId);
        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDeFromTableProperties(schema);

        String json = readFileToString(
                "BulkExportLeafPartitionQuerySerDeTest.shouldThrowExceptionNoPartitionRegion.json");

        // When & Then
        assertThatThrownBy(() -> querySerDe.fromJson(json))
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage("Query validation failed for export \"e-id\": partitionRegion field must be provided");
    }

    @Test
    public void shouldThrowExceptionNoFiles() throws IOException {
        // Given
        String tableId = "t-id";
        tableProperties.set(TABLE_ID, tableId);
        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDeFromTableProperties(schema);

        String json = readFileToString(
                "BulkExportLeafPartitionQuerySerDeTest.shouldThrowExceptionNoFiles.json");

        // When & Then
        assertThatThrownBy(() -> querySerDe.fromJson(json))
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage("Query validation failed for export \"e-id\": files field must be provided");
    }

    private BulkExportLeafPartitionQuerySerDe generateQuerySerDeFromTableProperties(Schema schema) {
        tableProperties.setSchema(schema);
        return new BulkExportLeafPartitionQuerySerDe(new FixedTablePropertiesProvider(tableProperties));
    }

    private BulkExportLeafPartitionQuerySerDe generateQuerySerDeFromSchema(Schema schema) {
        return new BulkExportLeafPartitionQuerySerDe(schema);
    }

    private String readFileToString(String path) throws IOException {
        try (Reader reader = new InputStreamReader(
                BulkExportLeafPartitionQuerySerDeTest.class.getClassLoader().getResourceAsStream(path))) {
            return CharStreams.toString(reader);
        }
    }
}
