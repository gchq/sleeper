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
package sleeper.query.runner.websocket;

import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.LegacyRetryStrategy;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.services.apigatewaymanagementapi.ApiGatewayManagementApiClient;
import software.amazon.awssdk.services.apigatewaymanagementapi.ApiGatewayManagementApiClientBuilder;
import software.amazon.awssdk.services.apigatewaymanagementapi.model.ForbiddenException;
import software.amazon.awssdk.services.apigatewaymanagementapi.model.GoneException;
import software.amazon.awssdk.services.apigatewaymanagementapi.model.LimitExceededException;
import software.amazon.awssdk.services.apigatewaymanagementapi.model.PayloadTooLargeException;

import sleeper.query.runner.output.WebSocketOutput;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Map;

public class ApiGatewayWebSocketOutput {

    private static final int DEFAULT_MAX_ATTEMPTS = 5;
    private static final double DEFAULT_THROTTLING_BASE_DELAY_SECS = 0.1;
    private static final double DEFAULT_THROTTLING_MAX_DELAY_SECS = 2.0;

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
                .endpointOverride(URI.create(endpoint))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryStrategy(retryStrategy(config))
                        .build());

        String accessKey = config.get(WebSocketOutput.ACCESS_KEY);
        String secretKey = config.get(WebSocketOutput.SECRET_KEY);
        if (StringUtils.isNotEmpty(accessKey) && StringUtils.isNotEmpty(secretKey)) {
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
            throw new IOException(e);
        }
    }

    private static LegacyRetryStrategy retryStrategy(Map<String, String> config) {
        // Tuned for a slow-consuming WebSocket client. The API Gateway Management API throttles per-connection;
        // we use a longer throttling backoff than the SDK defaults so the client has time to drain its buffer.
        double baseDelaySecs = WebSocketOutput.getDoubleOrDefault(config, WebSocketOutput.THROTTLING_RETRY_BASE_DELAY_SECS, DEFAULT_THROTTLING_BASE_DELAY_SECS);
        double maxDelaySecs = WebSocketOutput.getDoubleOrDefault(config, WebSocketOutput.THROTTLING_RETRY_MAX_DELAY_SECS, DEFAULT_THROTTLING_MAX_DELAY_SECS);
        int maxAttempts = WebSocketOutput.getIntOrDefault(config, WebSocketOutput.MAX_ATTEMPTS, DEFAULT_MAX_ATTEMPTS);
        return LegacyRetryStrategy.builder()
                .throttlingBackoffStrategy(BackoffStrategy.exponentialDelayHalfJitter(
                        Duration.ofMillis((long) (baseDelaySecs * 1000)),
                        Duration.ofMillis((long) (maxDelaySecs * 1000))))
                .maxAttempts(maxAttempts)
                .build();
    }

}
