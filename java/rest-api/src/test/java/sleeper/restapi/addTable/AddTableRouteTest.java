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

import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.schema.type.StringType;
import sleeper.core.table.TableStatus;
import sleeper.restapi.RestApiTestBase;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class AddTableRouteTest extends RestApiTestBase {
    private final Schema schema = createSchemaWithKey("key", new StringType());

    @Nested
    @DisplayName("Valid request tests")
    class ValidRequests {
        @Test
        void shouldAddTable() {
            // When
            APIGatewayV2HTTPResponse response = lambda.handleEvent(addTableEvent("""
                    {
                      "properties": {"sleeper.table.name": "my-table"},
                      "schema": %s
                    }
                    """.formatted(schemaJson(schema))));

            // Then
            assertThat(response.getStatusCode()).isEqualTo(201);
            assertThat(response.getBody()).contains("\"tableName\":\"my-table\"");

            assertThat(tablePropertiesStore.streamAllTableStatuses())
                    .flatExtracting(TableStatus::getTableName).containsExactly("my-table");
        }

        @Test
        void shouldAddTableWithSplitPoints() {
            // When
            APIGatewayV2HTTPResponse response = lambda.handleEvent(addTableEvent("""
                    {
                      "properties": {"sleeper.table.name": "my-table"},
                      "schema": %s,
                      "splitPoints": ["a", "m", "z"]
                    }
                    """.formatted(schemaJson(schema))));

            // Then
            assertThat(response.getStatusCode()).isEqualTo(201);

            TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
            tableProperties.set(TABLE_NAME, "my-table");
            assertThat(tablePropertiesStore.streamAllTableStatuses())
                    .flatExtracting(TableStatus::getTableName).containsExactly("my-table");
        }
    }

    @Test
    void shouldDecodeBase64EncodedBody() {
        // Given
        String body = """
                {"properties": {"sleeper.table.name": "my-table"}, "schema": %s}
                """.formatted(schemaJson(schema));
        String encoded = Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8));

        // When
        APIGatewayV2HTTPEvent event = event("POST", "/sleeper/tables", encoded);
        event.setIsBase64Encoded(true);

        // Then
        assertThat(lambda.handleEvent(event).getStatusCode()).isEqualTo(201);
    }

    @Nested
    @DisplayName("Request rejected tests")
    class RejectedValidAddTableRequests {
        @Test
        void shouldReturnReponseOfInvalidForMalformedJson() {
            // When
            APIGatewayV2HTTPResponse response = lambda.handleEvent(addTableEvent("not-json"));

            // Then
            assertThat(response.getStatusCode()).isEqualTo(400);
            assertThat(response.getBody()).contains("Request body is not valid JSON");
        }

        @Test
        void shouldReturnResponseForEmptyBody() {
            // When / Then
            assertThat(lambda.handleEvent(addTableEvent(null)).getStatusCode())
                    .isEqualTo(400);
        }

        @Test
        void shouldReturnRepsonseForMissingProperties() {
            // When
            APIGatewayV2HTTPResponse response = lambda.handleEvent(addTableEvent("""
                    {"schema": %s}
                    """.formatted(schemaJson(schema))));

            // Then
            assertThat(response.getStatusCode()).isEqualTo(400);
            assertThat(response.getBody()).contains("properties");
        }

        @Test
        void shouldReturnResponseWhenAddTableRejectsProperties() {
            // When
            APIGatewayV2HTTPResponse response = lambda.handleEvent(addTableEvent("""
                    {"properties": {"sleeper.table.dancemode": "flamenco"}, "schema": %s}
                    """.formatted(schemaJson(schema))));

            // Then
            assertThat(response.getStatusCode()).isEqualTo(400);
            assertThat(response.getBody()).contains("Property sleeper.table.name was invalid. It was unset");
        }

        @Test
        void shouldReturn409WhenTableAlreadyExists() {
            // Given
            APIGatewayV2HTTPEvent event = addTableEvent("""
                    {"properties": {"sleeper.table.name": "my-table"}, "schema": %s}
                    """.formatted(schemaJson(schema)));

            // When
            // Event sent once to establish table and then sent again for repeat effect
            lambda.handleEvent(event);

            // Then
            APIGatewayV2HTTPResponse response = lambda.handleEvent(event);
            assertThat(response.getStatusCode()).isEqualTo(409);
            assertThat(response.getBody()).contains("table_already_exists");
        }
    }

    private static String schemaJson(Schema schema) {
        return new SchemaSerDe().toJson(schema);
    }

    private APIGatewayV2HTTPEvent addTableEvent(String body) {
        return event("POST", "/sleeper/tables", body);
    }

}
