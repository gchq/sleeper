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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
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

import sleeper.clients.query.exception.MessageMalformedException;
import sleeper.clients.query.exception.MessageMissingFieldException;
import sleeper.clients.query.exception.UnknownMessageTypeException;
import sleeper.clients.query.exception.WebSocketClosedException;
import sleeper.clients.query.exception.WebSocketErrorException;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.util.LoggedDuration;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QuerySerDe;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL;
import static sleeper.core.properties.instance.CommonProperty.REGION;

public class QueryWebSocketClient {
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryWebSocketClient.class);
    public static final long DEFAULT_TIMEOUT_MS = 120000L;

    private final String apiUrl;
    private final Supplier<Client> clientSupplier;
    private final long timeoutMs;

    public QueryWebSocketClient(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider, AwsCredentialsProvider credentialsProvider) {
        this(instanceProperties, tablePropertiesProvider, credentialsProvider, DEFAULT_TIMEOUT_MS);
    }

    public QueryWebSocketClient(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider, AwsCredentialsProvider credentialsProvider, long timeoutMs) {
        this(instanceProperties, tablePropertiesProvider,
                clientSupplier(instanceProperties, tablePropertiesProvider, credentialsProvider), timeoutMs);
    }

    QueryWebSocketClient(InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            Supplier<Client> clientSupplier, long timeoutMs) {
        this.apiUrl = instanceProperties.get(CdkDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL);
        if (this.apiUrl == null) {
            throw new IllegalArgumentException("Use of this query client requires the WebSocket API to have been deployed as part of your Sleeper instance!");
        }
        this.clientSupplier = clientSupplier;
        this.timeoutMs = timeoutMs;
    }

    public CompletableFuture<List<String>> submitQuery(Query query) throws InterruptedException {
        Client client = clientSupplier.get();
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

    private static Supplier<Client> clientSupplier(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider, AwsCredentialsProvider credentialsProvider) {
        String region = instanceProperties.get(REGION);
        URI serverUri = URI.create(instanceProperties.get(QUERY_WEBSOCKET_API_URL));
        QuerySerDe serDe = new QuerySerDe(tablePropertiesProvider);
        return () -> {
            WebSocketMessageHandler messageHandler = new WebSocketMessageHandler(serDe);
            LOGGER.info("Obtaining AWS IAM credentials...");
            AwsCredentials credentials = credentialsProvider.resolveCredentials();
            return new WebSocketQueryClient(region, serverUri, credentials, messageHandler);
        };
    }

    private static class WebSocketQueryClient extends WebSocketClient implements Client {
        private final String region;
        private final URI serverUri;
        private final AwsCredentials credentials;
        private final WebSocketMessageHandler messageHandler;
        private Query query;

        private WebSocketQueryClient(String region, URI serverUri, AwsCredentials credentials, WebSocketMessageHandler messageHandler) {
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

    public static class WebSocketMessageHandler {
        private final Gson serde = new GsonBuilder().create();
        private final Set<String> outstandingQueries = new HashSet<>();
        private final Map<String, List<String>> subqueryIdByParentQueryId = new HashMap<>();
        private final Map<String, List<String>> records = new TreeMap<>();
        private final QuerySerDe querySerDe;
        private ClientCloser clientCloser;
        private boolean queryComplete = false;
        private boolean queryFailed = false;
        private long totalRecordsReturned = 0L;
        private CompletableFuture<List<String>> future;
        private String currentQueryId;

        public WebSocketMessageHandler(QuerySerDe querySerDe) {
            this.querySerDe = querySerDe;
        }

        public void setCloser(ClientCloser clientCloser) {
            this.clientCloser = clientCloser;
        }

        public void setFuture(CompletableFuture<List<String>> future) {
            this.future = future;
        }

        public void onOpen(Query query, Consumer<String> messageSender) {
            LOGGER.info("Connected to WebSocket API");
            String queryJson = querySerDe.toJson(query);
            LOGGER.info("Submitting Query: {}", queryJson);
            messageSender.accept(queryJson);
            outstandingQueries.add(query.getQueryId());
            subqueryIdByParentQueryId.put(query.getQueryId(), new ArrayList<>());
            currentQueryId = query.getQueryId();
        }

        public void onMessage(String json) {
            Optional<JsonObject> messageOpt = deserialiseMessage(json);
            if (!messageOpt.isPresent()) {
                close();
                return;
            }
            JsonObject message = messageOpt.get();
            String messageType = message.get("message").getAsString();
            String queryId = message.get("queryId").getAsString();

            if ("error".equals(messageType)) {
                handleError(message, queryId);
            } else if ("subqueries".equals(messageType)) {
                handleSubqueries(message, queryId);
            } else if ("rows".equals(messageType)) {
                handleRows(message, queryId);
            } else if ("completed".equals(messageType)) {
                handleCompleted(message, queryId);
            } else {
                queryFailed = true;
                future.completeExceptionally(new UnknownMessageTypeException(messageType));
                close();
            }

            if (outstandingQueries.isEmpty()) {
                queryComplete = true;
                future.complete(getResults(currentQueryId));
                close();
            }
        }

        private Optional<JsonObject> deserialiseMessage(String json) {
            try {
                JsonObject message = serde.fromJson(json, JsonObject.class);
                if (!message.has("queryId")) {
                    queryFailed = true;
                    future.completeExceptionally(new MessageMissingFieldException("queryId", json));
                    return Optional.empty();
                }
                if (!message.has("message")) {
                    queryFailed = true;
                    future.completeExceptionally(new MessageMissingFieldException("message", json));
                    return Optional.empty();
                }
                return Optional.of(message);
            } catch (JsonSyntaxException e) {
                queryFailed = true;
                future.completeExceptionally(new MessageMalformedException(json));
                return Optional.empty();
            }
        }

        private void handleError(JsonObject message, String queryId) {
            String error = message.get("error").getAsString();
            outstandingQueries.remove(queryId);
            queryFailed = true;
            future.completeExceptionally(new WebSocketErrorException(error));
            close();
        }

        private void handleSubqueries(JsonObject message, String queryId) {
            JsonArray subQueryIdList = message.getAsJsonArray("queryIds");
            LOGGER.info("Query {} split into the following subQueries:", queryId);
            List<String> subQueryIds = subQueryIdList.asList().stream().map(JsonElement::getAsString).collect(Collectors.toList());
            for (String subQueryId : subQueryIds) {
                LOGGER.info("  " + subQueryId);
                outstandingQueries.add(subQueryId);
            }
            outstandingQueries.remove(queryId);
            subqueryIdByParentQueryId.compute(queryId, (query, subQueries) -> {
                subQueries.addAll(subQueryIds);
                return subQueries;
            });
        }

        private void handleRows(JsonObject message, String queryId) {
            JsonArray recordBatch = message.getAsJsonArray("rows");
            List<String> recordList = recordBatch.asList().stream()
                    .map(jsonElement -> jsonElement.getAsJsonObject())
                    .map(JsonObject::toString)
                    .collect(Collectors.toList());
            if (!records.containsKey(queryId)) {
                records.put(queryId, recordList);
            } else {
                records.get(queryId).addAll(recordList);
            }
        }

        private void handleCompleted(JsonObject message, String queryId) {
            long recordCountFromApi = message.get("rowCount").getAsLong();
            boolean recordsReturnedToClient = false;
            for (JsonElement location : message.getAsJsonArray("locations")) {
                if (location.getAsJsonObject().get("type").getAsString().equals("websocket-endpoint")) {
                    recordsReturnedToClient = true;
                }
            }
            long returnedRecordCount = records.getOrDefault(queryId, List.of()).size();
            if (recordsReturnedToClient && recordCountFromApi > 0) {
                if (returnedRecordCount != recordCountFromApi) {
                    LOGGER.error("API said it had returned {} records for query {}, but only received {}",
                            recordCountFromApi, queryId, returnedRecordCount);
                }
            }
            outstandingQueries.remove(queryId);
            LOGGER.info("{} records returned by query {}. Remaining pending queries: {}",
                    recordCountFromApi, queryId, outstandingQueries.size());
            totalRecordsReturned += returnedRecordCount;
        }

        public void onClose(String reason) {
            LOGGER.info("Disconnected from WebSocket API: {}", reason);
            queryComplete = true;
            future.completeExceptionally(new WebSocketClosedException(reason));
        }

        public void onError(Exception error) {
            queryFailed = true;
            future.completeExceptionally(new WebSocketErrorException(error));
            close();
        }

        public boolean hasQueryFinished() {
            return queryComplete || queryFailed;
        }

        public long getTotalRecordsReturned() {
            return totalRecordsReturned;
        }

        public List<String> getResults(String queryId) {
            return Stream.concat(
                    Stream.of(queryId),
                    subqueryIdByParentQueryId.getOrDefault(queryId, List.of()).stream())
                    .flatMap(id -> records.getOrDefault(id, List.of()).stream())
                    .collect(Collectors.toList());
        }

        private void close() {
            LOGGER.info("Query finished, closing client");
            try {
                clientCloser.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }
}
