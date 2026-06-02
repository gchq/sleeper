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

import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.schema.type.StringType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

class RestApiLambdaTest extends RestApiTestBase {

    private final Schema schema = createSchemaWithKey("key", new StringType());

    @Nested
    @DisplayName("Failure response for REST API")
    class FailureResponses {
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

        @Test
        void shouldReturn500ForUnexpectedFailure() {
            doThrow(new RuntimeException("kaboom"))
                    .when(addTable).addTable(any(), any());

            APIGatewayV2HTTPResponse response = lambda.handleEvent(event("POST", "/sleeper/tables", """
                    {"properties": {"sleeper.table.name": "my-table"}, "schema": %s}
                    """.formatted(new SchemaSerDe().toJson(schema))));

            assertThat(response.getStatusCode()).isEqualTo(500);
            assertThat(response.getBody()).contains("internal_error");
            assertThat(response.getBody()).doesNotContain("kaboom");
        }
    }

}
