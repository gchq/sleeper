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
package sleeper.clients.query;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.internal.signer.DefaultSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;

import sleeper.core.properties.instance.InstanceProperties;

import java.net.URI;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL;
import static sleeper.core.properties.instance.CommonProperty.REGION;

class QueryWebSocketConnection extends WebSocketClient implements QueryWebSocketClient.Connection {
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryWebSocketConnection.class);

    private final String region;
    private final URI serverUri;
    private final AwsCredentials credentials;
    private final QueryWebSocketListener messageHandler;

    private QueryWebSocketConnection(String region, URI serverUri, AwsCredentials credentials, QueryWebSocketListener messageHandler) {
        super(serverUri);
        this.region = region;
        this.serverUri = serverUri;
        this.credentials = credentials;
        this.messageHandler = messageHandler;
    }

    public static QueryWebSocketConnection connect(
            InstanceProperties instanceProperties, QueryWebSocketListener messageHandler, AwsCredentialsProvider credentialsProvider) throws InterruptedException {
        String region = instanceProperties.get(REGION);
        URI serverUri = URI.create(instanceProperties.get(QUERY_WEBSOCKET_API_URL));
        LOGGER.info("Obtaining AWS IAM credentials...");
        AwsCredentials credentials = credentialsProvider.resolveCredentials();
        QueryWebSocketConnection connection = new QueryWebSocketConnection(region, serverUri, credentials, messageHandler);
        connection.initialiseConnection();
        return connection;
    }

    public static QueryWebSocketClient.Adapter createAdapter(AwsCredentialsProvider credentialsProvider) {
        return (instanceProperties, messageHandler) -> connect(instanceProperties, messageHandler, credentialsProvider);
    }

    private void initialiseConnection() throws InterruptedException {
        setAwsIamAuthHeaders();
        LOGGER.info("Connecting to WebSocket API at {}", serverUri);
        connectBlocking();
    }

    private void setAwsIamAuthHeaders() {
        LOGGER.debug("Creating auth signature with server URI: {}", serverUri);
        AwsV4HttpSigner signer = AwsV4HttpSigner.create();
        SignedRequest signed = signer.sign(DefaultSignRequest.builder(credentials)
                .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, "execute-api")
                .putProperty(AwsV4HttpSigner.REGION_NAME, region)
                .request(SdkHttpRequest.builder()
                        .uri(serverUri)
                        .protocol("https")
                        .method(SdkHttpMethod.GET)
                        .build())
                .build());
        LOGGER.debug("Setting auth headers...");
        signed.request().forEachHeader((header, values) -> {
            for (String value : values) {
                addHeader(header, value);
            }
        });
    }

    @Override
    public void onOpen(ServerHandshake handshake) {
        messageHandler.onOpen(this);
    }

    @Override
    public void onMessage(String json) {
        messageHandler.onMessage(json);
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        messageHandler.onClose(reason);
    }

    @Override
    public void onError(Exception error) {
        messageHandler.onError(error);
    }
}
