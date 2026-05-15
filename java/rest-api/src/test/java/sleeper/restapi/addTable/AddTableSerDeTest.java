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
package sleeper.restapi.addTable;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.SPLIT_POINTS_BASE64_ENCODED;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

class AddTableSerDeTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final AddTableSerDe addTableSerDe = new AddTableSerDe();

    @Nested
    @DisplayName("Valid add table requests")
    class ValidAddTableRequest {
        @Test
        void shouldBuildTablePropertiesAndApplySchema() {
            AddTableRequest request = addTableSerDe.fromJson("""
                    {
                      "properties": {"sleeper.table.name": "my-table"},
                      "schema": %s
                    }
                    """.formatted(schemaJson(createSchemaWithKey("key", new StringType()))));

            TableProperties tableProperties = request.toTableProperties(instanceProperties);

            assertThat(tableProperties.get(TABLE_NAME)).isEqualTo("my-table");
            assertThat(tableProperties.getSchema().getRowKeyFields()).hasSize(1);
            assertThat(request.toSplitPoints(tableProperties)).isEmpty();
        }

        @Test
        void shouldConvertIntSplitPoints() {
            TableProperties tableProperties = tablePropertiesWithSchema(createSchemaWithKey("key", new IntType()));

            AddTableRequest request = addTableSerDe.fromJson("""
                    {"properties": {}, "schema": {}, "splitPoints": ["1", "2", "3"]}
                    """);

            assertThat(request.toSplitPoints(tableProperties)).containsExactly(1, 2, 3);
        }

        @Test
        void shouldConvertLongSplitPoints() {
            TableProperties tableProperties = tablePropertiesWithSchema(createSchemaWithKey("key", new LongType()));

            AddTableRequest request = addTableSerDe.fromJson("""
                    {"properties": {}, "schema": {}, "splitPoints": ["10", "20"]}
                    """);

            assertThat(request.toSplitPoints(tableProperties)).containsExactly(10L, 20L);
        }

        @Test
        void shouldConvertStringSplitPoints() {
            TableProperties tableProperties = tablePropertiesWithSchema(createSchemaWithKey("key", new StringType()));

            AddTableRequest request = addTableSerDe.fromJson("""
                    {"properties": {}, "schema": {}, "splitPoints": ["a", "m", "z"]}
                    """);

            assertThat(request.toSplitPoints(tableProperties)).containsExactly("a", "m", "z");
        }

        @Test
        void shouldConvertBase64EncodedStringSplitPoints() {
            TableProperties tableProperties = tablePropertiesWithSchema(createSchemaWithKey("key", new StringType()));
            tableProperties.set(SPLIT_POINTS_BASE64_ENCODED, "true");
            String encoded = Base64.getEncoder().encodeToString("middle".getBytes(StandardCharsets.UTF_8));

            AddTableRequest request = addTableSerDe.fromJson("""
                    {"properties": {}, "schema": {}, "splitPoints": ["%s"]}
                    """.formatted(encoded));

            assertThat(request.toSplitPoints(tableProperties)).containsExactly("middle");
        }

        @Test
        void shouldConvertByteArraySplitPoints() {
            TableProperties tableProperties = tablePropertiesWithSchema(createSchemaWithKey("key", new ByteArrayType()));
            String encoded = Base64.getEncoder().encodeToString(new byte[]{1, 2, 3});

            AddTableRequest request = addTableSerDe.fromJson("""
                    {"properties": {}, "schema": {}, "splitPoints": ["%s"]}
                    """.formatted(encoded));

            assertThat(request.toSplitPoints(tableProperties))
                    .singleElement()
                    .isEqualTo(new byte[]{1, 2, 3});
        }
    }

    @Nested
    @DisplayName("Invalid add table request tests")
    class InvalidAddTableRequests {

        @Test
        void shouldRejectMissingProperties() {
            AddTableRequest request = addTableSerDe.fromJson("""
                    {"schema": {}}
                    """);

            assertThatThrownBy(() -> request.toTableProperties(instanceProperties))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("properties");
        }

        @Test
        void shouldRejectMissingSchema() {
            AddTableRequest request = addTableSerDe.fromJson("""
                    {"properties": {}}
                    """);

            assertThatThrownBy(() -> request.toTableProperties(instanceProperties))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("schema");
        }
    }

    @Nested
    @DisplayName("Valid add table response")
    class ValidAddTableResponse {
        @Test
        void shouldAddTableResponseCorrectlyConvertedtoJson() {
            String responseJson = addTableSerDe.toJson(AddTableResponse.builder()
                    .tableId("table-id")
                    .tableName("table-name")
                    .build());

            assertThat(responseJson).isEqualTo("{\"tableId\":\"table-id\",\"tableName\":\"table-name\"}");
        }
    }

    private TableProperties tablePropertiesWithSchema(Schema schema) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        return tableProperties;
    }

    private static String schemaJson(Schema schema) {
        return new SchemaSerDe().toJson(schema);
    }
}
