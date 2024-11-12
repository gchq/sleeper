/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.query.runner.output;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.apigatewaymanagementapi.AmazonApiGatewayManagementApi;
import com.amazonaws.services.apigatewaymanagementapi.AmazonApiGatewayManagementApiClientBuilder;
import com.amazonaws.services.apigatewaymanagementapi.model.ForbiddenException;
import com.amazonaws.services.apigatewaymanagementapi.model.GoneException;
import com.amazonaws.services.apigatewaymanagementapi.model.LimitExceededException;
import com.amazonaws.services.apigatewaymanagementapi.model.PayloadTooLargeException;
import com.amazonaws.services.apigatewaymanagementapi.model.PostToConnectionRequest;

import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryOrLeafPartitionQuery;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class WebSocketOutput {
    public static final String DESTINATION_NAME = "WEBSOCKET";
    public static final String REGION = "webSocketManagementApiRegion";
    public static final String ENDPOINT = "webSocketManagementApiEndpoint";
    public static final String CONNECTION_ID = "webSocketConnectionId";
    public static final String ACCESS_KEY = "awsAccessKey";
    public static final String SECRET_KEY = "awsSecretKey";
    public static final int MAX_PAYLOAD_SIZE = 128 * 1024;

    private final String endpoint;
    private final AmazonApiGatewayManagementApi client;
    private final String connectionId;
    private boolean clientGone = false;

    public WebSocketOutput(String awsRegion, String endpoint, String connectionId) {
        this(awsRegion, endpoint, connectionId, null);
    }

    public WebSocketOutput(String awsRegion, String endpoint, String connectionId, AWSCredentials awsCredentials) {
        this.endpoint = endpoint;
        this.connectionId = connectionId;
        AmazonApiGatewayManagementApiClientBuilder clientBuilder = AmazonApiGatewayManagementApiClientBuilder.standard()
                .withEndpointConfiguration(new EndpointConfiguration(this.endpoint, awsRegion));
        if (awsCredentials != null) {
            clientBuilder.withCredentials(new AWSStaticCredentialsProvider(awsCredentials));
        }
        this.client = clientBuilder.build();
    }

    public WebSocketOutput(Map<String, String> config) {
        String region = config.get(REGION);
        if (region == null) {
            region = System.getenv("AWS_REGION");
        }
        if (region == null) {
            throw new RuntimeException("Unable to detect current region!");
        }

        this.endpoint = config.get(ENDPOINT);
        if (this.endpoint == null) {
            throw new IllegalArgumentException("Missing WebSocket endpoint configuration");
        }

        this.connectionId = config.get(CONNECTION_ID);
        if (this.connectionId == null) {
            throw new IllegalArgumentException("Missing connectionId configuration");
        }

        AmazonApiGatewayManagementApiClientBuilder clientBuilder = AmazonApiGatewayManagementApiClientBuilder.standard()
                .withEndpointConfiguration(new EndpointConfiguration(this.endpoint, region));

        String accessKey = config.get(ACCESS_KEY);
        String secretKey = config.get(SECRET_KEY);
        if (accessKey != null && secretKey != null && !accessKey.isEmpty() && !secretKey.isEmpty()) {
            BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
            clientBuilder.setCredentials(new AWSStaticCredentialsProvider(awsCredentials));
        }

        this.client = clientBuilder.build();
    }

    public boolean isClientGone() {
        return clientGone;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getConnectionId() {
        return connectionId;
    }

    public void sendString(String message) throws IOException {
        if (clientGone) {
            throw new IOException("Not sending message as websocket client " + connectionId + " has already disconnected!");
        }

        PostToConnectionRequest request = new PostToConnectionRequest()
                .withConnectionId(connectionId)
                .withData(ByteBuffer.wrap(message.getBytes(StandardCharsets.UTF_8)));

        try {
            client.postToConnection(request);
        } catch (GoneException e) {
            clientGone = true;
            throw new IOException(e);
        } catch (LimitExceededException | PayloadTooLargeException | ForbiddenException e) {
            // Convert to checked exception
            throw new IOException(e);
        }
    }

    public static String getQueryId(QueryOrLeafPartitionQuery query) {
        if (query.isLeafQuery()) {
            return getQueryId(query.asLeafQuery());
        } else {
            return getQueryId(query.asParentQuery());
        }
    }

    public static String getQueryId(LeafPartitionQuery query) {
        return query.getSubQueryId();
    }

    public static String getQueryId(Query query) {
        return query.getQueryId();
    }
}
