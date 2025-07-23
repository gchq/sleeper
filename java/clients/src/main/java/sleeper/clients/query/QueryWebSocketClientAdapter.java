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
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.internal.signer.DefaultSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;

import sleeper.clients.query.QueryWebSocketClient.Client;
import sleeper.query.core.model.Query;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;

class QueryWebSocketClientAdapter extends WebSocketClient implements Client {
    private final String region;
    private final URI serverUri;
    private final AwsCredentials credentials;
    private final QueryWebSocketMessageHandler messageHandler;
    private Query query;

    QueryWebSocketClientAdapter(String region, URI serverUri, AwsCredentials credentials, QueryWebSocketMessageHandler messageHandler) {
        super(serverUri);
        this.region = region;
        this.serverUri = serverUri;
        this.credentials = credentials;
        this.messageHandler = messageHandler;
        messageHandler.setCloser(this::closeBlocking);
    }

    public CompletableFuture<List<String>> startQueryFuture(Query query) throws InterruptedException {
        CompletableFuture<List<String>> future = new CompletableFuture<>();
        messageHandler.setFuture(future);
        this.query = query;
        initialiseConnection();
        return future;
    }

    private void initialiseConnection() throws InterruptedException {
        setAwsIamAuthHeaders();
        QueryWebSocketClient.LOGGER.info("Connecting to WebSocket API at {}", serverUri);
        connectBlocking();
    }

    private void setAwsIamAuthHeaders() {
        QueryWebSocketClient.LOGGER.debug("Creating auth signature with server URI: {}", serverUri);
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
        QueryWebSocketClient.LOGGER.debug("Setting auth headers...");
        signed.request().forEachHeader((header, values) -> {
            for (String value : values) {
                addHeader(header, value);
            }
        });
    }

    public boolean hasQueryFinished() {
        return messageHandler.hasQueryFinished();
    }

    public long getTotalRecordsReturned() {
        return messageHandler.getTotalRecordsReturned();
    }

    @Override
    public void onOpen(ServerHandshake handshake) {
        messageHandler.onOpen(query, this::send);
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

    @Override
    public List<String> getResults(String queryId) {
        return messageHandler.getResults(queryId);
    }
}
