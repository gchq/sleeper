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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.api.SleeperClient;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.restapi.addTable.AddTableRest;

import java.util.HashMap;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * A lambda that controls a REST API for Sleeper. Dispatches API Gateway requests to the {@link Route} registered
 * against the matching HTTP method and path.
 */
public class RestApiLambda {

    public static final Logger LOGGER = LoggerFactory.getLogger(RestApiLambda.class);

    private final SleeperClient sleeperClient;
    private final InstanceProperties instanceProperties;
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
        routes.put("POST /sleeper/tables", event -> AddTableRest.builder()
                .instanceProperties(instanceProperties)
                .sleeperClient(sleeperClient)
                .build().processRequest(event));
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
            return RestMethod.errorResponse(404, "not_found", "No route registered for " + routeKey);
        }
        return route.handle(event);
    }

    private static String methodAndPath(APIGatewayV2HTTPEvent event) {
        APIGatewayV2HTTPEvent.RequestContext.Http http = event.getRequestContext().getHttp();
        return http.getMethod() + " " + http.getPath();
    }

}
