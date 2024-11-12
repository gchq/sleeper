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
package sleeper.clients;

import com.amazonaws.DefaultRequest;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.HttpMethodName;
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

import sleeper.clients.exception.MessageMalformedException;
import sleeper.clients.exception.MessageMissingFieldException;
import sleeper.clients.exception.UnknownMessageTypeException;
import sleeper.clients.exception.WebSocketClosedException;
import sleeper.clients.exception.WebSocketErrorException;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.util.LoggedDuration;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QuerySerDe;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL;

public class QueryWebSocketClient {
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryWebSocketClient.class);
    private final String apiUrl;
    private final Supplier<Client> clientSupplier;
    private Client client;

    public QueryWebSocketClient(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider) {
        this(instanceProperties, tablePropertiesProvider,
                () -> new WebSocketQueryClient(instanceProperties, tablePropertiesProvider));
    }

    QueryWebSocketClient(InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            Supplier<Client> clientSupplier) {
        this.apiUrl = instanceProperties.get(CdkDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL);
        if (this.apiUrl == null) {
            throw new IllegalArgumentException("Use of this query client requires the WebSocket API to have been deployed as part of your Sleeper instance!");
        }
        this.clientSupplier = clientSupplier;
    }

    public CompletableFuture<List<String>> submitQuery(Query query) throws InterruptedException {
        client = clientSupplier.get();
        try {
            Instant startTime = Instant.now();
            return client.startQueryFuture(query)
                    .whenComplete((records, exception) -> {
                        LoggedDuration duration = LoggedDuration.withFullOutput(startTime, Instant.now());
                        long recordsReturned = client.getTotalRecordsReturned();
                        LOGGER.info("Query took {} to return {} records", duration, recordsReturned);
                    });
        } catch (Exception e) {
            try {
                client.closeBlocking();
            } catch (InterruptedException e2) {
                throw e2;
            }
            throw e;
        }
    }

    public interface Client {
        void closeBlocking() throws InterruptedException;

        CompletableFuture<List<String>> startQueryFuture(Query query) throws InterruptedException;

        boolean hasQueryFinished();

        long getTotalRecordsReturned();

        List<String> getResults(String queryId);
    }

    private static class WebSocketQueryClient extends WebSocketClient implements Client {
        private final WebSocketMessageHandler messageHandler;
        private final URI serverUri;
        private Query query;

        private WebSocketQueryClient(InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider) {
            this(URI.create(instanceProperties.get(QUERY_WEBSOCKET_API_URL)),
                    new WebSocketMessageHandler(new QuerySerDe(tablePropertiesProvider)));
        }

        private WebSocketQueryClient(URI serverUri, WebSocketMessageHandler messageHandler) {
            super(serverUri);
            this.serverUri = serverUri;
            this.messageHandler = messageHandler;
            messageHandler.setCloser(this::closeBlocking);
        }

        public CompletableFuture<List<String>> startQueryFuture(Query query) throws InterruptedException {
            CompletableFuture<List<String>> future = new CompletableFuture<>();
            messageHandler.setFuture(future);
            this.query = query;
            initialiseConnection(serverUri);
            return future;
        }

        private void initialiseConnection(URI serverUri) throws InterruptedException {
            try {
                Map<String, String> authHeaders = this.getAwsIamAuthHeaders(serverUri);
                for (Entry<String, String> header : authHeaders.entrySet()) {
                    this.addHeader(header.getKey(), header.getValue());
                }
            } catch (URISyntaxException e) {
                System.err.println(e);
            }
            LOGGER.info("Connecting to WebSocket API at " + serverUri);
            connectBlocking();
        }

        private Map<String, String> getAwsIamAuthHeaders(URI serverUri) throws URISyntaxException {
            LOGGER.info("Obtaining AWS IAM creds...");
            AWSCredentials creds = DefaultAWSCredentialsProviderChain.getInstance().getCredentials();

            DefaultRequest<Object> request = new DefaultRequest<>("execute-api");
            request.setHttpMethod(HttpMethodName.GET);
            request.setEndpoint(new URI(serverUri.getScheme() + "://" + serverUri.getAuthority()));
            request.setResourcePath(serverUri.getPath());

            AWS4Signer signer = new AWS4Signer();
            signer.setServiceName("execute-api");
            signer.sign(request, creds);

            return request.getHeaders();
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

            if (messageType.equals("error")) {
                handleError(message, queryId);
            } else if (messageType.equals("subqueries")) {
                handleSubqueries(message, queryId);
            } else if (messageType.equals("records")) {
                handleRecords(message, queryId);
            } else if (messageType.equals("completed")) {
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

        private void handleRecords(JsonObject message, String queryId) {
            JsonArray recordBatch = message.getAsJsonArray("records");
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
            long recordCountFromApi = message.get("recordCount").getAsLong();
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
