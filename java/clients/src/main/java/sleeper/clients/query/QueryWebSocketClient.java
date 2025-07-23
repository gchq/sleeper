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
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.util.LoggedDuration;
import sleeper.query.core.model.Query;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL;
import static sleeper.core.properties.instance.CommonProperty.REGION;

public class QueryWebSocketClient {
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryWebSocketClient.class);
    public static final long DEFAULT_TIMEOUT_MS = 120000L;

    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final ClientProvider clientProvider;
    private final long timeoutMs;

    public QueryWebSocketClient(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider, AwsCredentialsProvider credentialsProvider) {
        this(instanceProperties, tablePropertiesProvider, clientProvider(credentialsProvider), DEFAULT_TIMEOUT_MS);
    }

    QueryWebSocketClient(InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            ClientProvider clientProvider, long timeoutMs) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.clientProvider = clientProvider;
        this.timeoutMs = timeoutMs;
        if (!instanceProperties.isSet(QUERY_WEBSOCKET_API_URL)) {
            throw new IllegalArgumentException("Use of this query client requires the WebSocket API to have been deployed as part of your Sleeper instance.");
        }
    }

    public CompletableFuture<List<String>> submitQuery(Query query) throws InterruptedException {
        TableProperties tableProperties = tablePropertiesProvider.getByName(query.getTableName());
        Client client = clientProvider.createClient(instanceProperties, tableProperties);
        try {
            Instant startTime = Instant.now();
            return client.startQueryFuture(query)
                    .orTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                    .whenComplete((records, exception) -> {
                        LoggedDuration duration = LoggedDuration.withFullOutput(startTime, Instant.now());
                        long recordsReturned = client.getTotalRecordsReturned();
                        LOGGER.info("Query took {} to return {} records", duration, recordsReturned);
                        client.close();
                    });
        } catch (RuntimeException | InterruptedException e) {
            client.close();
            throw e;
        }
    }

    public interface Client {
        void close();

        CompletableFuture<List<String>> startQueryFuture(Query query) throws InterruptedException;

        boolean hasQueryFinished();

        long getTotalRecordsReturned();

        List<String> getResults(String queryId);
    }

    private static ClientProvider clientProvider(AwsCredentialsProvider credentialsProvider) {
        return (instanceProperties, tableProperties) -> {
            String region = instanceProperties.get(REGION);
            URI serverUri = URI.create(instanceProperties.get(QUERY_WEBSOCKET_API_URL));
            QueryWebSocketMessageHandler messageHandler = new QueryWebSocketMessageHandler(tableProperties.getSchema());
            LOGGER.info("Obtaining AWS IAM credentials...");
            AwsCredentials credentials = credentialsProvider.resolveCredentials();
            return new WebSocketQueryClient(region, serverUri, credentials, messageHandler);
        };
    }

    public interface ClientProvider {
        Client createClient(InstanceProperties instanceProperties, TableProperties tableProperties);
    }

    private static class WebSocketQueryClient extends WebSocketClient implements Client {
        private final String region;
        private final URI serverUri;
        private final AwsCredentials credentials;
        private final QueryWebSocketMessageHandler messageHandler;
        private Query query;

        private WebSocketQueryClient(String region, URI serverUri, AwsCredentials credentials, QueryWebSocketMessageHandler messageHandler) {
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

    public interface ClientCloser {
        void close() throws InterruptedException;
    }
}
