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
import org.mockito.ArgumentCaptor;

import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.SleeperProperty;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.schema.type.StringType;
import sleeper.core.table.TableAlreadyExistsException;
import sleeper.core.table.TableStatus;
import sleeper.restapi.RestApiTestHelper;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class AddTableTest extends RestApiTestHelper {
    private final Schema schema = createSchemaWithKey("key", new StringType());

    @Nested
    @DisplayName("Valid request tests")
    class ValidRequests {
        @Test
        void shouldAddTable() {
            APIGatewayV2HTTPResponse response = lambda.handleEvent(addTableEvent("""
                    {
                      "properties": {"sleeper.table.name": "my-table"},
                      "schema": %s
                    }
                    """.formatted(schemaJson(schema))));

            assertThat(response.getStatusCode()).isEqualTo(201);
            assertThat(response.getBody()).contains("\"tableName\":\"my-table\"");
            ArgumentCaptor<TableProperties> tableCaptor = ArgumentCaptor.forClass(TableProperties.class);
            @SuppressWarnings("unchecked")
            ArgumentCaptor<List<Object>> splitsCaptor = ArgumentCaptor.forClass(List.class);
            verify(addTable).addTable(tableCaptor.capture(), splitsCaptor.capture());
            assertThat(tableCaptor.getValue().get(sleeper.core.properties.table.TableProperty.TABLE_NAME))
                    .isEqualTo("my-table");
            assertThat(splitsCaptor.getValue()).isEmpty();
        }

        @Test
        void shouldAddTableWithSplitPoints() {
            APIGatewayV2HTTPResponse response = lambda.handleEvent(addTableEvent("""
                    {
                      "properties": {"sleeper.table.name": "my-table"},
                      "schema": %s,
                      "splitPoints": ["a", "m", "z"]
                    }
                    """.formatted(schemaJson(schema))));

            assertThat(response.getStatusCode()).isEqualTo(201);
            @SuppressWarnings("unchecked")
            ArgumentCaptor<List<Object>> splitsCaptor = ArgumentCaptor.forClass(List.class);
            verify(addTable).addTable(any(), splitsCaptor.capture());
            assertThat(splitsCaptor.getValue()).containsExactly("a", "m", "z");
        }
    }

    @Test
    void shouldDecodeBase64EncodedBody() {
        String body = """
                {"properties": {"sleeper.table.name": "my-table"}, "schema": %s}
                """.formatted(schemaJson(schema));
        String encoded = Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8));
        APIGatewayV2HTTPEvent event = event("POST", "/sleeper/tables", encoded);
        event.setIsBase64Encoded(true);

        APIGatewayV2HTTPResponse response = lambda.handleEvent(event);

        assertThat(response.getStatusCode()).isEqualTo(201);
    }

    @Nested
    @DisplayName("Request rejected tests")
    class RejectedValidAddTableRequests {
        @Test
        void shouldReturnReponseOfInvalidForMalformedJson() {
            APIGatewayV2HTTPResponse response = lambda.handleEvent(addTableEvent("not-json"));

            assertThat(response.getStatusCode()).isEqualTo(400);
            assertThat(response.getBody()).contains("invalid_request");
            verify(addTable, never()).addTable(any(), any());
        }

        @Test
        void shouldReturnResponseForEmptyBody() {
            APIGatewayV2HTTPResponse response = lambda.handleEvent(addTableEvent(null));

            assertThat(response.getStatusCode()).isEqualTo(400);
            verify(addTable, never()).addTable(any(), any());
        }

        @Test
        void shouldReturnRepsonseForMissingProperties() {
            APIGatewayV2HTTPResponse response = lambda.handleEvent(addTableEvent("""
                    {"schema": %s}
                    """.formatted(schemaJson(schema))));

            assertThat(response.getStatusCode()).isEqualTo(400);
            assertThat(response.getBody()).contains("properties");
            verify(addTable, never()).addTable(any(), any());
        }

        @Test
        void shouldReturnResponseWhenAddTableRejectsProperties() {
            Map<SleeperProperty, String> invalidValues = Map.of(CdkDefinedInstanceProperty.ACCOUNT, "Failure");
            doThrow(new SleeperPropertiesInvalidException(invalidValues))
                    .when(addTable).addTable(any(), any());

            APIGatewayV2HTTPResponse response = lambda.handleEvent(addTableEvent("""
                    {"properties": {"sleeper.table.name": "my-table"}, "schema": %s}
                    """.formatted(schemaJson(schema))));

            assertThat(response.getStatusCode()).isEqualTo(400);
        }

        @Test
        void shouldReturn409WhenTableAlreadyExists() {
            TableStatus existing = TableStatus.uniqueIdAndName("table-id", "my-table", true);
            doThrow(new TableAlreadyExistsException(existing))
                    .when(addTable).addTable(any(), any());

            APIGatewayV2HTTPResponse response = lambda.handleEvent(addTableEvent("""
                    {"properties": {"sleeper.table.name": "my-table"}, "schema": %s}
                    """.formatted(schemaJson(schema))));

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
