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
package sleeper.query.runnerv2.output;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.apigatewaymanagementapi.ApiGatewayManagementApiClient;
import software.amazon.awssdk.services.apigatewaymanagementapi.ApiGatewayManagementApiClientBuilder;
import software.amazon.awssdk.services.apigatewaymanagementapi.model.ForbiddenException;
import software.amazon.awssdk.services.apigatewaymanagementapi.model.GoneException;
import software.amazon.awssdk.services.apigatewaymanagementapi.model.LimitExceededException;
import software.amazon.awssdk.services.apigatewaymanagementapi.model.PayloadTooLargeException;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class ApiGatewayWebSocketOutput {

    private final ApiGatewayManagementApiClient client;
    private final String connectionId;
    private boolean clientGone = false;

    public ApiGatewayWebSocketOutput(ApiGatewayManagementApiClient client, String connectionId) {
        this.client = client;
        this.connectionId = connectionId;
    }

    public static ApiGatewayWebSocketOutput fromConfig(Map<String, String> config) {
        String region = config.get(WebSocketOutput.REGION);
        if (region == null) {
            region = System.getenv("AWS_REGION");
        }
        if (region == null) {
            throw new RuntimeException("Unable to detect current region!");
        }

        String endpoint = config.get(WebSocketOutput.ENDPOINT);
        if (endpoint == null) {
            throw new IllegalArgumentException("Missing WebSocket endpoint configuration");
        }

        String connectionId = config.get(WebSocketOutput.CONNECTION_ID);
        if (connectionId == null) {
            throw new IllegalArgumentException("Missing connectionId configuration");
        }

        ApiGatewayManagementApiClientBuilder clientBuilder = ApiGatewayManagementApiClient.builder()
                .region(Region.of(region))
                .endpointOverride(URI.create(endpoint));

        String accessKey = config.get(WebSocketOutput.ACCESS_KEY);
        String secretKey = config.get(WebSocketOutput.SECRET_KEY);
        if (accessKey != null && secretKey != null && !accessKey.isEmpty() && !secretKey.isEmpty()) {
            clientBuilder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey)));
        }

        return new ApiGatewayWebSocketOutput(clientBuilder.build(), connectionId);
    }

    public void sendString(String message) throws IOException {
        if (clientGone) {
            throw new IOException("Not sending message as websocket client " + connectionId + " has already disconnected!");
        }

        try {
            client.postToConnection(request -> request
                    .connectionId(connectionId)
                    .data(SdkBytes.fromUtf8String(message)));
        } catch (GoneException e) {
            clientGone = true;
            throw new IOException(e);
        } catch (LimitExceededException | PayloadTooLargeException | ForbiddenException e) {
            // Convert to checked exception
            throw new IOException(e);
        }
    }

}
