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
package sleeper.restapi;

import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent.RequestContext;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent.RequestContext.Http;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import sleeper.clients.api.SleeperClient;
import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.SleeperProperty;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.schema.type.StringType;
import sleeper.core.table.TableAlreadyExistsException;
import sleeper.core.table.TableStatus;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

class RestApiLambdaTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final SleeperClient sleeperClient = mock(SleeperClient.class);
    private final Schema schema = createSchemaWithKey("key", new StringType());
    private RestApiLambda lambda;

    @BeforeEach
    void setUp() {
        lambda = new RestApiLambda(instanceProperties, sleeperClient);
    }

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
        verify(sleeperClient).addTable(tableCaptor.capture(), splitsCaptor.capture());
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
        verify(sleeperClient).addTable(any(), splitsCaptor.capture());
        assertThat(splitsCaptor.getValue()).containsExactly("a", "m", "z");
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

    @Test
    void shouldReturn400ForMalformedJson() {
        APIGatewayV2HTTPResponse response = lambda.handleEvent(addTableEvent("not-json"));

        assertThat(response.getStatusCode()).isEqualTo(400);
        assertThat(response.getBody()).contains("invalid_request");
        verify(sleeperClient, never()).addTable(any(), any());
    }

    @Test
    void shouldReturn400ForEmptyBody() {
        APIGatewayV2HTTPResponse response = lambda.handleEvent(addTableEvent(null));

        assertThat(response.getStatusCode()).isEqualTo(400);
        verify(sleeperClient, never()).addTable(any(), any());
    }

    @Test
    void shouldReturn400ForMissingProperties() {
        APIGatewayV2HTTPResponse response = lambda.handleEvent(addTableEvent("""
                {"schema": %s}
                """.formatted(schemaJson(schema))));

        assertThat(response.getStatusCode()).isEqualTo(400);
        assertThat(response.getBody()).contains("properties");
        verify(sleeperClient, never()).addTable(any(), any());
    }

    @Test
    void shouldReturn400WhenSleeperClientRejectsProperties() {
        Map<SleeperProperty, String> invalidValues = Map.of(CdkDefinedInstanceProperty.ACCOUNT, "Failure");
        doThrow(new SleeperPropertiesInvalidException(invalidValues))
                .when(sleeperClient).addTable(any(), any());

        APIGatewayV2HTTPResponse response = lambda.handleEvent(addTableEvent("""
                {"properties": {"sleeper.table.name": "my-table"}, "schema": %s}
                """.formatted(schemaJson(schema))));

        assertThat(response.getStatusCode()).isEqualTo(400);
    }

    @Test
    void shouldReturn409WhenTableAlreadyExists() {
        TableStatus existing = TableStatus.uniqueIdAndName("table-id", "my-table", true);
        doThrow(new TableAlreadyExistsException(existing))
                .when(sleeperClient).addTable(any(), any());

        APIGatewayV2HTTPResponse response = lambda.handleEvent(addTableEvent("""
                {"properties": {"sleeper.table.name": "my-table"}, "schema": %s}
                """.formatted(schemaJson(schema))));

        assertThat(response.getStatusCode()).isEqualTo(409);
        assertThat(response.getBody()).contains("table_already_exists");
    }

    @Test
    void shouldReturn500ForUnexpectedFailure() {
        doThrow(new RuntimeException("kaboom"))
                .when(sleeperClient).addTable(any(), any());

        APIGatewayV2HTTPResponse response = lambda.handleEvent(addTableEvent("""
                {"properties": {"sleeper.table.name": "my-table"}, "schema": %s}
                """.formatted(schemaJson(schema))));

        assertThat(response.getStatusCode()).isEqualTo(500);
        assertThat(response.getBody()).contains("internal_error");
        assertThat(response.getBody()).doesNotContain("kaboom");
    }

    @Test
    void shouldReturn404ForUnknownRoute() {
        APIGatewayV2HTTPResponse response = lambda.handleEvent(event("GET", "/sleeper/unknown", null));

        assertThat(response.getStatusCode()).isEqualTo(404);
    }

    @Test
    void shouldRespondToExistingPlaceholderRoute() {
        APIGatewayV2HTTPResponse response = lambda.handleEvent(event("GET", "/sleeper", null));

        assertThat(response.getStatusCode()).isEqualTo(200);
    }

    private APIGatewayV2HTTPEvent addTableEvent(String body) {
        return event("POST", "/sleeper/tables", body);
    }

    private APIGatewayV2HTTPEvent event(String method, String path, String body) {
        Http http = new Http();
        http.setMethod(method);
        http.setPath(path);
        RequestContext requestContext = new RequestContext();
        requestContext.setHttp(http);
        APIGatewayV2HTTPEvent event = new APIGatewayV2HTTPEvent();
        event.setRequestContext(requestContext);
        event.setBody(body);
        return event;
    }

    private static String schemaJson(Schema schema) {
        return new SchemaSerDe().toJson(schema);
    }
}
