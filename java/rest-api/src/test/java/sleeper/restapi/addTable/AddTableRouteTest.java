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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStoreException;
import sleeper.restapi.RestApiTestBase;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class AddTableRouteTest extends RestApiTestBase {
    private final Schema schema = createSchemaWithKey("key", new StringType());
    private final TableProperties tableProperties = new TableProperties(instanceProperties);

    @BeforeEach
    void setUp() {
        tableProperties.set(TABLE_NAME, "test-table");
        tableProperties.setSchema(schema);
    }

    @Nested
    @DisplayName("Valid request tests")
    class ValidRequests {
        @Test
        void shouldAddTable() {
            // When
            tableProperties.set(TABLE_NAME, "my-table");
            APIGatewayV2HTTPResponse response = lambda.handleEvent(addTableEvent());

            // Then
            assertThat(response.getStatusCode()).isEqualTo(201);
            assertThat(response.getBody()).contains("\"tableName\":\"my-table\"");
            assertThat(tablePropertiesStore.streamAllTables())
                    .containsExactly(withTableIdFromStore(tableProperties));
        }

        @Test
        void shouldAddTableWithSplitPoints() throws StateStoreException {
            // When
            APIGatewayV2HTTPResponse response = lambda.handleEvent(addTableEventWithSplitPoints(List.of("a", "m", "z")));

            // Then
            assertThat(response.getStatusCode()).isEqualTo(201);
            TableProperties expectedProperties = withTableIdFromStore(tableProperties);
            assertThat(tablePropertiesStore.streamAllTables()).containsExactly(expectedProperties);
            assertThat(stateStoreProvider.getStateStore(expectedProperties).getLeafPartitions())
                    .extracting(partition -> partition.getRegion().getRange("key").getMin())
                    .containsExactlyInAnyOrder("", "a", "m", "z");
        }
    }

    @Test
    void shouldDecodeBase64EncodedBody() {
        // Given
        tableProperties.set(TABLE_NAME, "my-table");
        String encoded = Base64.getEncoder().encodeToString(addTableBody().getBytes(StandardCharsets.UTF_8));

        APIGatewayV2HTTPEvent event = event("POST", "/sleeper/tables", encoded);
        event.setIsBase64Encoded(true);

        // When
        APIGatewayV2HTTPResponse response = lambda.handleEvent(event);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(201);
        assertThat(response.getBody()).contains("tableName\":\"my-table");
    }

    @Nested
    @DisplayName("Request rejected tests")
    class RejectedAddTableRequests {
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
            // Given
            tableProperties.unset(TABLE_NAME);

            // When
            APIGatewayV2HTTPResponse response = lambda.handleEvent(addTableEvent());

            // Then
            assertThat(response.getStatusCode()).isEqualTo(400);
            assertThat(response.getBody()).contains("Property sleeper.table.name was invalid. It was unset");
        }

        @Test
        void shouldReturn409WhenTableAlreadyExists() {
            // Given
            APIGatewayV2HTTPEvent event = addTableEvent();
            lambda.handleEvent(event);

            // When
            APIGatewayV2HTTPResponse response = lambda.handleEvent(event);

            // Then
            assertThat(response.getStatusCode()).isEqualTo(409);
            assertThat(response.getBody()).contains("table_already_exists");
        }
    }

    private static String schemaJson(Schema schema) {
        return new SchemaSerDe().toJson(schema);
    }

    private APIGatewayV2HTTPEvent addTableEvent() {
        return addTableEvent(addTableBody());
    }

    private APIGatewayV2HTTPEvent addTableEventWithSplitPoints(List<Object> splitPoints) {
        AddTableRequest request = AddTableRequest.builder().properties(tableProperties).splitPoints(splitPoints).build();
        return addTableEvent(new AddTableRequestSerDe(instanceProperties).toJson(request));
    }

    private String addTableBody() {
        AddTableRequest request = AddTableRequest.builder().properties(tableProperties).build();
        return new AddTableRequestSerDe(instanceProperties).toJson(request);
    }

    private TableProperties withTableIdFromStore(TableProperties tableProperties) {
        String tableId = tablePropertiesStore.loadByName(tableProperties.get(TABLE_NAME)).get(TABLE_ID);
        TableProperties newProperties = TableProperties.copyOf(tableProperties);
        newProperties.set(TABLE_ID, tableId);
        return newProperties;
    }

    private APIGatewayV2HTTPEvent addTableEvent(String body) {
        return event("POST", "/sleeper/tables", body);
    }

}
