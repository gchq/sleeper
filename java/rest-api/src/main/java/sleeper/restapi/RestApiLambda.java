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
import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.api.SleeperClient;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.table.TableAlreadyExistsException;
import sleeper.restapi.route.Route;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

/**
 * A lambda that controls a REST API for Sleeper. Dispatches API Gateway requests to the {@link Route} registered
 * against the matching HTTP method and path.
 */
public class RestApiLambda {

    public static final Logger LOGGER = LoggerFactory.getLogger(RestApiLambda.class);
    private static final String CONTENT_TYPE_JSON = "application/json";

    private final SleeperClient sleeperClient;
    private final InstanceProperties instanceProperties;
    private final Gson gson = new Gson();
    private final Map<String, Route> routes = new HashMap<>();

    public RestApiLambda() {
        String configBucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        if (configBucket == null) {
            throw new IllegalArgumentException(
                    "Couldn't get S3 bucket from environment variable " + CONFIG_BUCKET.toEnvironmentVariable());
        }
        S3Client s3Client = S3Client.create();
        InstanceProperties properties = S3InstanceProperties.loadFromBucket(s3Client, configBucket);
        this.instanceProperties = properties;
        this.sleeperClient = SleeperClient.builder().instanceProperties(properties).build();
        registerRoutes();
    }

    RestApiLambda(InstanceProperties instanceProperties, SleeperClient sleeperClient) {
        this.instanceProperties = instanceProperties;
        this.sleeperClient = sleeperClient;
        registerRoutes();
    }

    private void registerRoutes() {
        routes.put("GET /sleeper", event -> APIGatewayV2HTTPResponse.builder()
                .withStatusCode(200)
                .withBody("API successfully interacted with. Further expansion for functionality required.")
                .build());
        routes.put("POST /sleeper/tables", this::addTable);
    }

    /**
     * Used by the lambda handler to handle API Gateway requests that have been sent to the REST API.
     *
     * @param  event the event
     * @return       the response from the API request
     */
    public APIGatewayV2HTTPResponse handleEvent(APIGatewayV2HTTPEvent event) {
        LOGGER.info("REST API request received: {}", event);
        String routeKey = methodAndPath(event);
        Route route = routes.get(routeKey);
        if (route == null) {
            return errorResponse(404, "not_found", "No route registered for " + routeKey);
        }
        return route.handle(event);
    }

    private APIGatewayV2HTTPResponse addTable(APIGatewayV2HTTPEvent event) {
        AddTableRequest request;
        try {
            request = gson.fromJson(decodeBody(event), AddTableRequest.class);
        } catch (JsonSyntaxException e) {
            LOGGER.warn("Add table request body was not valid JSON", e);
            return errorResponse(400, "invalid_request", "Request body is not valid JSON");
        }
        if (request == null) {
            return errorResponse(400, "invalid_request", "Request body is empty");
        }
        TableProperties tableProperties;
        List<Object> splitPoints;
        try {
            tableProperties = request.toTableProperties(instanceProperties);
            splitPoints = request.toSplitPoints(tableProperties);
        } catch (IllegalArgumentException | RuntimeException e) {
            // SchemaSerDe / split-point parsing surface invalid input as runtime exceptions.
            LOGGER.warn("Add table request was invalid", e);
            return errorResponse(400, "invalid_request", e.getMessage());
        }
        try {
            sleeperClient.addTable(tableProperties, splitPoints);
        } catch (TableAlreadyExistsException e) {
            LOGGER.info("Add table request rejected: table already exists", e);
            return errorResponse(409, "table_already_exists", e.getMessage());
        } catch (IllegalArgumentException e) {
            // SleeperPropertiesInvalidException extends IllegalArgumentException.
            LOGGER.warn("Add table request rejected: properties failed validation", e);
            return errorResponse(400, "invalid_request", e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error("Add table request failed unexpectedly", e);
            return errorResponse(500, "internal_error", "Failed to add table");
        }
        Map<String, String> body = new HashMap<>();
        body.put("tableId", tableProperties.get(TABLE_ID));
        body.put("tableName", tableProperties.get(TABLE_NAME));
        return APIGatewayV2HTTPResponse.builder()
                .withStatusCode(201)
                .withHeaders(Map.of("Content-Type", CONTENT_TYPE_JSON))
                .withBody(gson.toJson(body))
                .build();
    }

    private static String methodAndPath(APIGatewayV2HTTPEvent event) {
        APIGatewayV2HTTPEvent.RequestContext.Http http = event.getRequestContext().getHttp();
        return http.getMethod() + " " + http.getPath();
    }

    private static String decodeBody(APIGatewayV2HTTPEvent event) {
        String body = event.getBody();
        if (body == null) {
            return "";
        }
        if (Boolean.TRUE.equals(event.getIsBase64Encoded())) {
            return new String(Base64.getDecoder().decode(body), StandardCharsets.UTF_8);
        }
        return body;
    }

    private APIGatewayV2HTTPResponse errorResponse(int status, String error, String message) {
        Map<String, String> body = new HashMap<>();
        body.put("error", error);
        if (message != null) {
            body.put("message", message);
        }
        return APIGatewayV2HTTPResponse.builder()
                .withStatusCode(status)
                .withHeaders(Map.of("Content-Type", CONTENT_TYPE_JSON))
                .withBody(gson.toJson(body))
                .build();
    }
}
