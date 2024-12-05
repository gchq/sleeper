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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
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

        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDe(schema, true);

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
        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDe(schema, true);

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
        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDe(schema, true);

        String json = readFileToString(
                "src/test/java/sleeper/bulkexport/model/BulkExportLeafPartitionQuerySerDeTest.shouldThrowExceptionNoExportId.approved.json");

        // When & Then
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
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
        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDe(schema, true);

        String json = readFileToString(
                "src/test/java/sleeper/bulkexport/model/BulkExportLeafPartitionQuerySerDeTest.shouldThrowExceptionNoSubExportId.approved.json");

        // When & Then
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
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
        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDe(schema, true);

        String json = readFileToString(
                "src/test/java/sleeper/bulkexport/model/BulkExportLeafPartitionQuerySerDeTest.shouldThrowExceptionNoRegions.approved.json");

        // When & Then
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
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
        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDe(schema, true);

        String json = readFileToString(
                "src/test/java/sleeper/bulkexport/model/BulkExportLeafPartitionQuerySerDeTest.shouldThrowExceptionNoLeafPartitionId.approved.json");

        // When & Then
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
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
        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDe(schema, true);

        String json = readFileToString(
                "src/test/java/sleeper/bulkexport/model/BulkExportLeafPartitionQuerySerDeTest.shouldThrowExceptionNoPartitionRegion.approved.json");

        // When & Then
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
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
        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDe(schema, true);

        String json = readFileToString(
                "src/test/java/sleeper/bulkexport/model/BulkExportLeafPartitionQuerySerDeTest.shouldThrowExceptionNoFiles.approved.json");

        // When & Then
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
        assertThatThrownBy(() -> querySerDe.fromJson(json))
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage("Query validation failed for export \"e-id\": files field must be provided");
    }

    private BulkExportLeafPartitionQuerySerDe generateQuerySerDe(Schema schema,
            boolean useTablePropertiesProvider) {
        if (useTablePropertiesProvider) {
            tableProperties.setSchema(schema);
            return new BulkExportLeafPartitionQuerySerDe(new FixedTablePropertiesProvider(tableProperties));
        }
        return new BulkExportLeafPartitionQuerySerDe(schema);
    }

    private String readFileToString(String path) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, StandardCharsets.UTF_8);
    }
}
