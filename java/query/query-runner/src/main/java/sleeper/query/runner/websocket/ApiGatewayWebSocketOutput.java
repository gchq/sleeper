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
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.conditions.AndRetryCondition;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.apigatewaymanagementapi.ApiGatewayManagementApiClient;
import software.amazon.awssdk.services.apigatewaymanagementapi.ApiGatewayManagementApiClientBuilder;
import software.amazon.awssdk.services.apigatewaymanagementapi.model.ForbiddenException;
import software.amazon.awssdk.services.apigatewaymanagementapi.model.GoneException;
import software.amazon.awssdk.services.apigatewaymanagementapi.model.LimitExceededException;
import software.amazon.awssdk.services.apigatewaymanagementapi.model.PayloadTooLargeException;

import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ExponentialBackoffWithJitter.WaitRange;
import sleeper.core.util.ThreadSleep;
import sleeper.query.runner.output.WebSocketOutput;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class ApiGatewayWebSocketOutput {

    private static final int DEFAULT_MAX_ATTEMPTS = 5;
    private static final double DEFAULT_FIRST_WAIT_CEILING_SECS = 0.1;
    private static final double DEFAULT_MAX_WAIT_CEILING_SECS = 2.0;

    private final ApiGatewayManagementApiClient client;
    private final String connectionId;
    private final ExponentialBackoffWithJitter backoff;
    private final int maxAttempts;
    private boolean clientGone = false;

    public ApiGatewayWebSocketOutput(
            ApiGatewayManagementApiClient client, String connectionId,
            ExponentialBackoffWithJitter backoff, int maxAttempts) {
        this.client = client;
        this.connectionId = connectionId;
        this.backoff = backoff;
        this.maxAttempts = maxAttempts;
    }

    public static ApiGatewayWebSocketOutput fromConfig(Map<String, String> config) {
        return fromConfig(config, Thread::sleep);
    }

    public static ApiGatewayWebSocketOutput fromConfig(Map<String, String> config, ThreadSleep waiter) {
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
                        .retryPolicy(retryPolicyExcludingLimitExceeded())
                        .build());

        String accessKey = config.get(WebSocketOutput.ACCESS_KEY);
        String secretKey = config.get(WebSocketOutput.SECRET_KEY);
        if (StringUtils.isNotEmpty(accessKey) && StringUtils.isNotEmpty(secretKey)) {
            clientBuilder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey)));
        }

        double firstWaitCeilingSecs = configDouble(config, WebSocketOutput.LIMIT_EXCEEDED_FIRST_WAIT_CEILING_SECS, DEFAULT_FIRST_WAIT_CEILING_SECS);
        double maxWaitCeilingSecs = configDouble(config, WebSocketOutput.LIMIT_EXCEEDED_MAX_WAIT_CEILING_SECS, DEFAULT_MAX_WAIT_CEILING_SECS);
        int maxAttempts = configInt(config, WebSocketOutput.MAX_ATTEMPTS, DEFAULT_MAX_ATTEMPTS);
        ExponentialBackoffWithJitter backoff = new ExponentialBackoffWithJitter(
                WaitRange.firstAndMaxWaitCeilingSecs(firstWaitCeilingSecs, maxWaitCeilingSecs),
                Math::random, waiter);

        return new ApiGatewayWebSocketOutput(clientBuilder.build(), connectionId, backoff, maxAttempts);
    }

    public void sendString(String message) throws IOException {
        if (clientGone) {
            throw new IOException("Not sending message as websocket client " + connectionId + " has already disconnected!");
        }

        LimitExceededException lastLimitExceeded = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                backoff.waitBeforeAttempt(attempt);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            try {
                client.postToConnection(request -> request
                        .connectionId(connectionId)
                        .data(SdkBytes.fromUtf8String(message)));
                return;
            } catch (GoneException e) {
                clientGone = true;
                throw new IOException(e);
            } catch (LimitExceededException e) {
                lastLimitExceeded = e;
            } catch (PayloadTooLargeException | ForbiddenException e) {
                throw new IOException(e);
            }
        }
        throw new IOException(lastLimitExceeded);
    }

    private static RetryPolicy retryPolicyExcludingLimitExceeded() {
        return RetryPolicy.defaultRetryPolicy().toBuilder()
                .retryCondition(AndRetryCondition.create(
                        RetryCondition.defaultRetryCondition(),
                        context -> !(context.exception() instanceof LimitExceededException)))
                .build();
    }

    private static double configDouble(Map<String, String> config, String key, double defaultValue) {
        String value = config.get(key);
        return value == null ? defaultValue : Double.parseDouble(value);
    }

    private static int configInt(Map<String, String> config, String key, int defaultValue) {
        String value = config.get(key);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

}
