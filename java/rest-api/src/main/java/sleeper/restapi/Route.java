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
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;

/**
 * Handles a single REST API route. Implementations are mapped against a HTTP method and path by
 * {@link sleeper.restapi.RestApiLambda}.
 */
@FunctionalInterface
public interface Route {

    /**
     * Handles a request matched to this route.
     *
     * @param  event the API Gateway request event
     * @return       the response to return to API Gateway
     */
    APIGatewayV2HTTPResponse handle(APIGatewayV2HTTPEvent event);
}
