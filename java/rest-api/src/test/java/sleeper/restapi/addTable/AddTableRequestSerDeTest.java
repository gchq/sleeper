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

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

class AddTableRequestSerDeTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final AddTableRequestSerDe serDe = new AddTableRequestSerDe();

    @Nested
    @DisplayName("Deserialise add table requests")
    class DeserialiseRequests {
        @Test
        void shouldBuildTablePropertiesAndApplySchema() {
            // Given
            Schema schema = createSchemaWithKey("key", new StringType());
            AddTableRequest request = serDe.fromJson("""
                    {
                      "properties": {"sleeper.table.name": "my-table"},
                      "schema": %s
                    }
                    """.formatted(schemaJson(schema)));

            // When
            TableProperties tableProperties = request.toTableProperties(instanceProperties);

            // Then
            TableProperties expectedTableProperties = new TableProperties(instanceProperties);
            expectedTableProperties.set(TABLE_NAME, "my-table");
            expectedTableProperties.setSchema(schema);
            assertThat(tableProperties).isEqualTo(expectedTableProperties);
            assertThat(request.toSplitPoints(tableProperties)).isEmpty();
        }

        @Test
        void shouldConvertIntSplitPoints() {
            TableProperties tableProperties = tablePropertiesWithSchema(createSchemaWithKey("key", new IntType()));

            AddTableRequest request = serDe.fromJson("""
                    {"properties": {}, "schema": {}, "splitPoints": ["1", "2", "3"]}
                    """);

            assertThat(request.toSplitPoints(tableProperties)).containsExactly(1, 2, 3);
        }

        @Test
        void shouldConvertLongSplitPoints() {
            TableProperties tableProperties = tablePropertiesWithSchema(createSchemaWithKey("key", new LongType()));

            AddTableRequest request = serDe.fromJson("""
                    {"properties": {}, "schema": {}, "splitPoints": ["10", "20"]}
                    """);

            assertThat(request.toSplitPoints(tableProperties)).containsExactly(10L, 20L);
        }

        @Test
        void shouldConvertStringSplitPoints() {
            TableProperties tableProperties = tablePropertiesWithSchema(createSchemaWithKey("key", new StringType()));

            AddTableRequest request = serDe.fromJson("""
                    {"properties": {}, "schema": {}, "splitPoints": ["a", "m", "z"]}
                    """);

            assertThat(request.toSplitPoints(tableProperties)).containsExactly("a", "m", "z");
        }

        @Test
        void shouldConvertByteArraySplitPoints() {
            TableProperties tableProperties = tablePropertiesWithSchema(createSchemaWithKey("key", new ByteArrayType()));
            String encoded = Base64.getEncoder().encodeToString(new byte[]{1, 2, 3});

            AddTableRequest request = serDe.fromJson("""
                    {"properties": {}, "schema": {}, "splitPoints": ["%s"]}
                    """.formatted(encoded));

            assertThat(request.toSplitPoints(tableProperties))
                    .singleElement()
                    .isEqualTo(new byte[]{1, 2, 3});
        }
    }

    @Nested
    @DisplayName("Invalid add table requests")
    class InvalidRequests {

        @Test
        void shouldRejectMissingProperties() {
            AddTableRequest request = serDe.fromJson("""
                    {"schema": {}}
                    """);

            assertThatThrownBy(() -> request.toTableProperties(instanceProperties))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Request must include 'properties'");
        }

        @Test
        void shouldRejectMissingSchema() {
            AddTableRequest request = serDe.fromJson("""
                    {"properties": {}}
                    """);

            assertThatThrownBy(() -> request.toTableProperties(instanceProperties))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Request must include 'schema'");
        }
    }

    @Nested
    @DisplayName("Serialise add table requests")
    class SerialiseRequests {
        @Test
        void shouldRoundTripRequestWithPropertiesAndSchema() {
            // Given
            Schema schema = createSchemaWithKey("key", new StringType());
            AddTableRequest request = serDe.fromJson("""
                    {
                      "properties": {"sleeper.table.name": "my-table"},
                      "schema": %s
                    }
                    """.formatted(schemaJson(schema)));

            // When
            AddTableRequest deserialisedRequest = serDe.fromJson(serDe.toJson(request));

            // Then
            TableProperties tableProperties = deserialisedRequest.toTableProperties(instanceProperties);
            TableProperties expectedTableProperties = new TableProperties(instanceProperties);
            expectedTableProperties.set(TABLE_NAME, "my-table");
            expectedTableProperties.setSchema(schema);
            assertThat(tableProperties).isEqualTo(expectedTableProperties);
        }

        @Test
        void shouldRoundTripRequestWithSplitPoints() {
            // Given
            AddTableRequest request = serDe.fromJson("""
                    {"properties": {}, "schema": {}, "splitPoints": ["1", "2", "3"]}
                    """);

            // When
            AddTableRequest deserialisedRequest = serDe.fromJson(serDe.toJson(request));

            // Then
            TableProperties tableProperties = tablePropertiesWithSchema(createSchemaWithKey("key", new IntType()));
            assertThat(deserialisedRequest.toSplitPoints(tableProperties)).containsExactly(1, 2, 3);
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
