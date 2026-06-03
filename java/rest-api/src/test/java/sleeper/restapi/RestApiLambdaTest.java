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

import static org.assertj.core.api.Assertions.assertThat;

class RestApiLambdaTest extends RestApiTestBase {

    @Nested
    @DisplayName("Failure response for REST API")
    class FailureResponses {
        @Test
        void shouldReturn404ForUnknownRoute() {
            APIGatewayV2HTTPResponse response = lambda.handleEvent(event("GET", "/sleeper/unknown", null));

            assertThat(response.getStatusCode()).isEqualTo(404);
        }
    }

}
