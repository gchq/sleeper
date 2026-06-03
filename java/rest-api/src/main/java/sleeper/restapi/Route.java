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
import com.google.gson.Gson;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles a single REST API route. Implementations are mapped against a HTTP method and path by
 * {@link sleeper.restapi.RestApiLambda}.
 */
@FunctionalInterface
public interface Route {

    String CONTENT_TYPE_JSON = "application/json";

    /**
     * Handles a request matched to this route.
     *
     * @param  event the API Gateway request event
     * @return       the response to return to API Gateway
     */
    APIGatewayV2HTTPResponse handle(APIGatewayV2HTTPEvent event);

    /**
     * Provides body element for use within method requested.
     *
     * @param  event Request object containing details to be extracted
     * @return       Element containing all details from within the event relevant to the method requested.
     */
    static String decodeBody(APIGatewayV2HTTPEvent event) {
        String body = event.getBody();
        if (body == null) {
            return "";
        }
        if (Boolean.TRUE.equals(event.getIsBase64Encoded())) {
            return new String(Base64.getDecoder().decode(body), StandardCharsets.UTF_8);
        }
        return body;
    }

    /**
     * Building error response to return out given that something has failed whilst processing request.
     *
     * @param  status  HTTP status code to report
     * @param  error   Enumeration of the type of error thrown
     * @param  message Further details of the error to provide context
     * @return         An APIGateway response to be relayed back out of application
     */
    static APIGatewayV2HTTPResponse errorResponse(int status, String error, String message) {
        Map<String, String> body = new HashMap<>();
        body.put("error", error);
        if (message != null) {
            body.put("message", message);
        }
        return APIGatewayV2HTTPResponse.builder()
                .withStatusCode(status)
                .withHeaders(Map.of("Content-Type", CONTENT_TYPE_JSON))
                .withBody(new Gson().toJson(body))
                .build();
    }
}
