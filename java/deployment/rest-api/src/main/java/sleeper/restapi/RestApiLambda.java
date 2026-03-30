/*
 * Copyright 2022-2025 Crown Copyright
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A lambda that controls a REST API for Sleeper.
 */
public class RestApiLambda {
    public static final Logger LOGGER = LoggerFactory.getLogger(RestApiLambda.class);

    public RestApiLambda() {
    }

    /**
     * Used by the lambda handler to handle API Gateway requests that have been sent to the REST API.
     * Stub method to be expanded later.
     *
     * @param  event the event
     * @return       the response from the API request
     */
    public APIGatewayV2HTTPResponse handleEvent(APIGatewayV2HTTPEvent event) {
        LOGGER.info(String.format("REST API Request recieved: %s", event.toString()));
        return APIGatewayV2HTTPResponse.builder()
                .withBody("API successfully interacted with. Further expansion for functionality required.")
                .build();

    }
}
