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

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
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
    private final String tableId = tableProperties.get(TABLE_ID);
    private final String exportId = UUID.randomUUID().toString();
    private final String subExportId = UUID.randomUUID().toString();
    private final String leafPartitionId = UUID.randomUUID().toString();

    @Test
    public void shouldSerDeBulkExportLeafPartitionQuery() {
        // Given
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

        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDe("my-table", schema, true);

        // When
        String json = querySerDe.toJson(bulkExportLeafPartitionQuery, true);

        BulkExportLeafPartitionQuery deserialisedQuery = querySerDe.fromJson(json);

        // Then
        String expectedJson = "{\n" +
                "  \"tableId\": \"" + tableId + "\",\n" +
                "  \"exportId\": \"" + exportId + "\",\n" +
                "  \"subExportId\": \"" + subExportId + "\",\n" +
                "  \"regions\": [\n" +
                "    {\n" +
                "      \"key\": {\n" +
                "        \"min\": 1,\n" +
                "        \"minInclusive\": true,\n" +
                "        \"max\": 10,\n" +
                "        \"maxInclusive\": true\n" +
                "      },\n" +
                "      \"stringsBase64Encoded\": true\n" +
                "    }\n" +
                "  ],\n" +
                "  \"leafPartitionId\": \"" + leafPartitionId + "\",\n" +
                "  \"partitionRegion\": {\n" +
                "    \"key\": {\n" +
                "      \"min\": 0,\n" +
                "      \"minInclusive\": true,\n" +
                "      \"max\": 1000,\n" +
                "      \"maxInclusive\": false\n" +
                "    },\n" +
                "    \"stringsBase64Encoded\": true\n" +
                "  },\n" +
                "  \"files\": [\n" +
                "    \"/test/file.parquet\"\n" +
                "  ]\n" +
                "}";
        assertThat(bulkExportLeafPartitionQuery).isEqualTo(deserialisedQuery);
        assertThat(json).isEqualTo(expectedJson);
    }

    @Test
    public void shouldThrowExceptionWithNullTableId() {
        // Given
        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDe("my-table", schema, true);

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
        // Given
        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDe("my-table", schema, true);

        String queryJson = "{\n" +
                "    \"tableId\":\"" + tableId + "\",\n" +
                "    \"subExportId\":\"" + subExportId + "\",\n" +
                "    \"regions\":[\n" +
                "        {\n" +
                "            \"key\":{\n" +
                "                \"min\":1,\n" +
                "                \"minInclusive\":true,\n" +
                "                \"max\":10,\n" +
                "                \"maxInclusive\":true\n" +
                "            },\n" +
                "            \"stringsBase64Encoded\":true\n" +
                "        }\n" +
                "    ],\n" +
                "    \"leafPartitionId\":\"" + leafPartitionId + "\",\n" +
                "    \"partitionRegion\":{\n" +
                "        \"key\":{\n" +
                "            \"min\":0,\n" +
                "            \"minInclusive\":true,\n" +
                "            \"max\":1000,\n" +
                "            \"maxInclusive\":false\n" +
                "        },\n" +
                "        \"stringsBase64Encoded\":true\n" +
                "    },\n" +
                "    \"files\":[\"/test/file.parquet\"]\n" +
                "}";

        // When & Then
        assertThatThrownBy(() -> querySerDe.fromJson(queryJson))
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage("Query validation failed: exportId field must be provided");
    }

    @Test
    public void shouldThrowExceptionNoSubExportId() {
        // Given
        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDe("my-table", schema, true);

        String queryJson = "{\n" +
                "    \"tableId\":\"" + tableId + "\",\n" +
                "    \"exportId\":\"" + exportId + "\",\n" +
                "    \"regions\":[\n" +
                "        {\n" +
                "            \"key\":{\n" +
                "                \"min\":1,\n" +
                "                \"minInclusive\":true,\n" +
                "                \"max\":10,\n" +
                "                \"maxInclusive\":true\n" +
                "            },\n" +
                "            \"stringsBase64Encoded\":true\n" +
                "        }\n" +
                "    ],\n" +
                "    \"leafPartitionId\":\"" + leafPartitionId + "\",\n" +
                "    \"partitionRegion\":{\n" +
                "        \"key\":{\n" +
                "            \"min\":0,\n" +
                "            \"minInclusive\":true,\n" +
                "            \"max\":1000,\n" +
                "            \"maxInclusive\":false\n" +
                "        },\n" +
                "        \"stringsBase64Encoded\":true\n" +
                "    },\n" +
                "    \"files\":[\"/test/file.parquet\"]\n" +
                "}";

        // When & Then
        assertThatThrownBy(() -> querySerDe.fromJson(queryJson))
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage(
                        "Query validation failed for export \"" + exportId + "\": subExportId field must be provided");
    }

    @Test
    public void shouldThrowExceptionNoLeafPartitionId() {
        // Given
        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDe("my-table", schema, true);

        String queryJson = "{\n" +
                "    \"tableId\":\"" + tableId + "\",\n" +
                "    \"exportId\":\"" + exportId + "\",\n" +
                "    \"subExportId\":\"" + subExportId + "\",\n" +
                "    \"regions\":[\n" +
                "        {\n" +
                "            \"key\":{\n" +
                "                \"min\":1,\n" +
                "                \"minInclusive\":true,\n" +
                "                \"max\":10,\n" +
                "                \"maxInclusive\":true\n" +
                "            },\n" +
                "            \"stringsBase64Encoded\":true\n" +
                "        }\n" +
                "    ],\n" +
                "    \"partitionRegion\":{\n" +
                "        \"key\":{\n" +
                "            \"min\":0,\n" +
                "            \"minInclusive\":true,\n" +
                "            \"max\":1000,\n" +
                "            \"maxInclusive\":false\n" +
                "        },\n" +
                "        \"stringsBase64Encoded\":true\n" +
                "    },\n" +
                "    \"files\":[\"/test/file.parquet\"]\n" +
                "}";

        // When & Then
        assertThatThrownBy(() -> querySerDe.fromJson(queryJson))
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage("Query validation failed for export \"" + exportId
                        + "\": leafPartitionId field must be provided");
    }

    @Test
    public void shouldThrowExceptionNoFiles() {
        // Given
        BulkExportLeafPartitionQuerySerDe querySerDe = generateQuerySerDe("my-table", schema, true);
        String queryJson = "{\n" +
                "    \"tableId\":\"" + tableId + "\",\n" +
                "    \"exportId\":\"" + exportId + "\",\n" +
                "    \"subExportId\":\"" + subExportId + "\",\n" +
                "    \"regions\":[\n" +
                "        {\n" +
                "            \"key\":{\n" +
                "                \"min\":1,\n" +
                "                \"minInclusive\":true,\n" +
                "                \"max\":10,\n" +
                "                \"maxInclusive\":true\n" +
                "            },\n" +
                "            \"stringsBase64Encoded\":true\n" +
                "        }\n" +
                "    ],\n" +
                "    \"leafPartitionId\":\"" + leafPartitionId + "\",\n" +
                "    \"partitionRegion\":{\n" +
                "        \"key\":{\n" +
                "            \"min\":0,\n" +
                "            \"minInclusive\":true,\n" +
                "            \"max\":1000,\n" +
                "            \"maxInclusive\":false\n" +
                "        },\n" +
                "        \"stringsBase64Encoded\":true\n" +
                "    }\n" +
                "}";
        // When & Then
        assertThatThrownBy(() -> querySerDe.fromJson(queryJson))
                .isInstanceOf(
                        BulkExportQueryValidationException.class)
                .hasMessage("Query validation failed for export \"" + exportId + "\": files field must be provided");
    }

    private BulkExportLeafPartitionQuerySerDe generateQuerySerDe(String tableName, Schema schema,
            boolean useTablePropertiesProvider) {
        if (useTablePropertiesProvider) {
            tableProperties.set(TABLE_NAME, tableName);
            tableProperties.setSchema(schema);
            return new BulkExportLeafPartitionQuerySerDe(new FixedTablePropertiesProvider(tableProperties));
        }
        return new BulkExportLeafPartitionQuerySerDe(schema);
    }
}
