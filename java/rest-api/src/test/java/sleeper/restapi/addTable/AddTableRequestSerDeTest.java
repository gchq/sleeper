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

import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.key.Key;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.KeyComparator;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.List;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

class AddTableRequestSerDeTest {

    private final InstanceProperties instanceProperties = new InstanceProperties();
    private final TableProperties tableProperties = new TableProperties(instanceProperties);
    private final AddTableRequestSerDe serDe = new AddTableRequestSerDe(instanceProperties);
    private List<Object> splitPoints = List.of();

    @BeforeEach
    void setUp() {
        tableProperties.set(TABLE_NAME, "my-table");
        tableProperties.setSchema(createSchemaWithKey("key", new StringType()));
    }

    @Nested
    @DisplayName("Serialise add table requests")
    class SerialiseRequests {
        @Test
        void shouldSerDePropertiesWithSchema() {
            // When
            AddTableRequest request = serDe.fromJson(serDe.toJson(createAddTableRequest()));

            // Then
            assertThat(request.getProperties()).isEqualTo(tableProperties);
            assertThat(request.getSplitPoints()).isEmpty();
        }

        @Test
        void shouldExcludeSchemaFromPropertiesJsonField() {
            // When
            String json = serDe.toJson(createAddTableRequest());

            // Then
            assertThatJson(json).inPath("$.properties")
                    .isEqualTo("{\"sleeper.table.name\":\"my-table\"}");
        }

        @Test
        void shouldSerialiseInExpectedJsonFormat() {
            Approvals.verify(
                    serDe.toJson(createAddTableRequest(), true),
                    new Options().forFile().withName("example", ".json"));
        }

        @Test
        void shouldDeserialiseWithNoSplitPoints() {
            // Given
            String json = """
                    {
                      "properties": {
                        "sleeper.table.name": "my-table"
                      },
                      "schema": %s
                    }
                    """.formatted(schemaJson(tableProperties.getSchema()));

            // When
            AddTableRequest request = serDe.fromJson(json);

            // Then
            assertThat(request.getProperties()).isEqualTo(tableProperties);
            assertThat(request.getSplitPoints()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Invalid add table requests")
    class InvalidRequests {

        @Test
        void shouldRejectMissingProperties() {
            String json = """
                    {"schema": %s}
                    """.formatted(schemaJson(tableProperties.getSchema()));

            assertThatThrownBy(() -> jsonToTableProperties(json))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("Request must include 'properties'");
        }

        @Test
        void shouldRejectMissingSchema() {
            String json = """
                    {"properties": {}}
                    """;

            assertThatThrownBy(() -> jsonToTableProperties(json))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("Request must include 'schema'");
        }

        @Test
        void shouldRejectNull() {
            assertThatThrownBy(() -> jsonToTableProperties("null"))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    @DisplayName("Serialise split points")
    class SerialiseSplitPoints {

        @Test
        void shouldSerDeStringSplitPoints() {
            // Given
            tableProperties.setSchema(createSchemaWithKey("key", new StringType()));
            splitPoints = List.of("g", "s");

            // When
            AddTableRequest found = serDe.fromJson(serDe.toJson(createAddTableRequest()));

            // Then
            assertThat(found.getSplitPoints()).isEqualTo(splitPoints);
        }

        @Test
        void shouldSerDeIntSplitPoints() {
            // Given
            tableProperties.setSchema(createSchemaWithKey("key", new IntType()));
            splitPoints = List.of(1, 2, 3);

            // When
            AddTableRequest found = serDe.fromJson(serDe.toJson(createAddTableRequest()));

            // Then
            assertThat(found.getSplitPoints()).isEqualTo(splitPoints);
        }

        @Test
        void shouldSerDeLongSplitPoints() {
            // Given
            tableProperties.setSchema(createSchemaWithKey("key", new LongType()));
            splitPoints = List.of(1L, 2L, 3L);

            // When
            AddTableRequest found = serDe.fromJson(serDe.toJson(createAddTableRequest()));

            // Then
            assertThat(found.getSplitPoints()).isEqualTo(splitPoints);
        }

        @Test
        void shouldSerDeByteArraySplitPoints() {
            // Given
            tableProperties.setSchema(createSchemaWithKey("key", new ByteArrayType()));
            splitPoints = List.of(new byte[]{1, 2, 3}, new byte[]{4, 5, 6});

            // When
            AddTableRequest found = serDe.fromJson(serDe.toJson(createAddTableRequest()));

            // Then
            assertThat(Key.create(found.getSplitPoints()))
                    .usingComparator(new KeyComparator(new ByteArrayType()))
                    .isEqualTo(Key.create(splitPoints));
        }

        @Test
        void shouldSerDeUnsetSplitPoints() {
            // Given
            splitPoints = null;

            // When
            AddTableRequest found = serDe.fromJson(serDe.toJson(createAddTableRequest()));

            // Then
            assertThat(found.getSplitPoints()).isEmpty();
        }
    }

    private AddTableRequest createAddTableRequest() {
        return AddTableRequest.builder()
                .properties(tableProperties)
                .splitPoints(splitPoints)
                .build();
    }

    private static String schemaJson(Schema schema) {
        return new SchemaSerDe().toJson(schema);
    }

    private TableProperties jsonToTableProperties(String json) {
        return serDe.fromJson(json).getProperties();
    }
}
