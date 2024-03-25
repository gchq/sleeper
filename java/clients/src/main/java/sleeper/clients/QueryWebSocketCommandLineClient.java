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
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.configuration.properties.instance.CdkDefinedInstanceProperty;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableIndex;
import sleeper.core.util.LoggedDuration;
import sleeper.query.model.Query;
import sleeper.query.model.QuerySerDe;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL;

public class QueryWebSocketCommandLineClient extends QueryCommandLineClient {
    private final String apiUrl;
    private final Client client;

    private QueryWebSocketCommandLineClient(
            InstanceProperties instanceProperties, TableIndex tableIndex, TablePropertiesProvider tablePropertiesProvider,
            ConsoleInput in, ConsoleOutput out) {
        this(instanceProperties, tableIndex, tablePropertiesProvider, in, out, new WebSocketQueryClient(instanceProperties, tablePropertiesProvider, out));
    }

    private QueryWebSocketCommandLineClient(
            InstanceProperties instanceProperties, TableIndex tableIndex, TablePropertiesProvider tablePropertiesProvider,
            ConsoleInput in, ConsoleOutput out, Client client) {
        this(instanceProperties, tableIndex, tablePropertiesProvider, in, out, client, () -> UUID.randomUUID().toString());
    }

    QueryWebSocketCommandLineClient(
            InstanceProperties instanceProperties, TableIndex tableIndex, TablePropertiesProvider tablePropertiesProvider,
            ConsoleInput in, ConsoleOutput out, Client client, Supplier<String> queryIdSupplier) {
        super(instanceProperties, tableIndex, tablePropertiesProvider, in, out, queryIdSupplier);

        this.apiUrl = instanceProperties.get(CdkDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL);
        if (this.apiUrl == null) {
            throw new IllegalArgumentException("Use of this query client requires the WebSocket API to have been deployed as part of your Sleeper instance!");
        }
        this.client = client;
    }

    @Override
    protected void init(TableProperties tableProperties) {
    }

    @Override
    protected void submitQuery(TableProperties tableProperties, Query query) {
        try {
            Instant startTime = Instant.now();
            client.startQuery(query);
            while (!client.hasQueryFinished()) {
                Thread.sleep(500);
            }
            LoggedDuration duration = LoggedDuration.withFullOutput(startTime, Instant.now());
            long recordsReturned = client.getTotalRecordsReturned();
            out.println("Query took " + duration + " to return " + recordsReturned + " records");
        } catch (InterruptedException e) {
        } finally {
            try {
                client.closeBlocking();
            } catch (InterruptedException e) {
            }
        }
    }

    public interface Client {
        void closeBlocking() throws InterruptedException;

        void startQuery(Query query) throws InterruptedException;

        boolean hasQueryFinished();

        long getTotalRecordsReturned();
    }

    private static class WebSocketQueryClient extends WebSocketClient implements Client {
        private final ConsoleOutput out;
        private final WebSocketMessageHandler messageHandler;
        private final URI serverUri;
        private Query query;

        private WebSocketQueryClient(InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider, ConsoleOutput out) {
            this(URI.create(instanceProperties.get(QUERY_WEBSOCKET_API_URL)),
                    out, new WebSocketMessageHandler(new QuerySerDe(tablePropertiesProvider), out));
        }

        private WebSocketQueryClient(URI serverUri, ConsoleOutput out, WebSocketMessageHandler messageHandler) {
            super(serverUri);
            this.serverUri = serverUri;
            this.out = out;
            this.messageHandler = messageHandler;
        }

        public void startQuery(Query query) throws InterruptedException {
            this.query = query;
            initialiseConnection(serverUri);
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
            out.println("Connecting to WebSocket API at " + serverUri);
            connectBlocking();
        }

        private Map<String, String> getAwsIamAuthHeaders(URI serverUri) throws URISyntaxException {
            out.println("Obtaining AWS IAM creds...");
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
    }

    public static class WebSocketMessageHandler {
        private final Gson serde = new GsonBuilder().create();
        private final Set<String> outstandingQueries = new HashSet<>();
        private final Map<String, List<String>> records = new TreeMap<>();
        private final QuerySerDe querySerDe;
        private final ConsoleOutput out;
        private boolean queryComplete = false;
        private boolean queryFailed = false;
        private long totalRecordsReturned = 0L;

        public WebSocketMessageHandler(QuerySerDe querySerDe, ConsoleOutput out) {
            this.querySerDe = querySerDe;
            this.out = out;
        }

        public void onOpen(Query query, Consumer<String> messageSender) {
            out.println("Connected to WebSocket API");
            String queryJson = querySerDe.toJson(query);
            out.println("Submitting Query: " + queryJson);
            messageSender.accept(queryJson);
            outstandingQueries.add(query.getQueryId());
        }

        public void onMessage(String json) {
            Optional<JsonObject> messageOpt = deserialiseMessage(json);
            if (!messageOpt.isPresent()) {
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
                out.println("Received unrecognised message type: " + messageType);
                queryFailed = true;
            }

            if (outstandingQueries.isEmpty()) {
                if (!records.isEmpty()) {
                    out.println("Query results:");
                    records.values().stream()
                            .flatMap(List::stream)
                            .forEach(record -> out.println(record));
                }
                queryComplete = true;
            }
        }

        private Optional<JsonObject> deserialiseMessage(String json) {
            try {
                JsonObject message = serde.fromJson(json, JsonObject.class);
                if (!message.has("queryId")) {
                    out.println("Received message without queryId from API:");
                    out.println("  " + json);
                    queryFailed = true;
                    return Optional.empty();
                }
                if (!message.has("message")) {
                    out.println("Received message without message type from API:");
                    out.println("  " + json);
                    queryFailed = true;
                    return Optional.empty();
                }
                return Optional.of(message);
            } catch (JsonSyntaxException e) {
                out.println("Received malformed JSON message from API:");
                out.println("  " + json);
                queryFailed = true;
                return Optional.empty();
            }
        }

        private void handleError(JsonObject message, String queryId) {
            out.println("Encountered an error while running query " + queryId + ": " + message.get("error").getAsString());
            outstandingQueries.remove(queryId);
            queryFailed = true;
        }

        private void handleSubqueries(JsonObject message, String queryId) {
            JsonArray subQueryIdList = message.getAsJsonArray("queryIds");
            out.println("Query " + queryId + " split into the following subQueries:");
            for (JsonElement subQueryIdElement : subQueryIdList) {
                String subQueryId = subQueryIdElement.getAsString();
                out.println("  " + subQueryId);
                outstandingQueries.add(subQueryId);
            }
            outstandingQueries.remove(queryId);

        }

        private void handleRecords(JsonObject message, String queryId) {
            JsonArray recordBatch = message.getAsJsonArray("records");
            List<String> recordList = recordBatch.asList().stream().map(JsonElement::getAsString).collect(Collectors.toList());
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
                    out.println("ERROR: API said it had returned " + recordCountFromApi + " records for query " + queryId + ", but only received " + returnedRecordCount);
                }
            }
            outstandingQueries.remove(queryId);
            out.println(recordCountFromApi + " records returned by query: " + queryId + ". Remaining pending queries: " + outstandingQueries.size());
            totalRecordsReturned += returnedRecordCount;
        }

        public void onClose(String reason) {
            out.println("Disconnected from WebSocket API: " + reason);
            queryComplete = true;
        }

        public void onError(Exception error) {
            out.println("Encountered an error: " + error.getMessage());
            queryFailed = true;
        }

        public boolean hasQueryFinished() {
            return queryComplete || queryFailed;
        }

        public long getTotalRecordsReturned() {
            return totalRecordsReturned;
        }

    }

    public static void main(String[] args) throws StateStoreException {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, args[0]);

        QueryWebSocketCommandLineClient client = new QueryWebSocketCommandLineClient(instanceProperties,
                new DynamoDBTableIndex(instanceProperties, dynamoDBClient),
                new TablePropertiesProvider(instanceProperties, amazonS3, dynamoDBClient),
                new ConsoleInput(System.console()), new ConsoleOutput(System.out));
        client.run();
    }
}
