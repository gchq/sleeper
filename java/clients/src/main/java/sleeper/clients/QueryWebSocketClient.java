/*
 * Copyright 2022-2023 Crown Copyright
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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import sleeper.clients.util.ClientUtils;
import sleeper.configuration.properties.instance.CdkDefinedInstanceProperty;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreException;
import sleeper.query.model.Query;
import sleeper.query.model.QuerySerDe;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class QueryWebSocketClient extends QueryCommandLineClient {
    private final String apiUrl;
    private final QuerySerDe querySerDe;

    protected QueryWebSocketClient(AmazonS3 s3Client, InstanceProperties instanceProperties) {
        super(s3Client, instanceProperties);

        this.apiUrl = instanceProperties.get(CdkDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL);
        if (this.apiUrl == null) {
            throw new IllegalArgumentException("Use of this query client requires the WebSocket API to have been deployed as part of your Sleeper instance!");
        }
        this.querySerDe = new QuerySerDe(new TablePropertiesProvider(s3Client, instanceProperties));
    }

    @Override
    protected void init(TableProperties tableProperties) {
    }

    @Override
    protected void submitQuery(TableProperties tableProperties, Query query) {
        Client client = null;
        try {
            long startTime = System.currentTimeMillis();
            client = new Client(URI.create(apiUrl), query, querySerDe);
            while (!client.isQueryComplete()) {
                Thread.sleep(500);
            }
            double delta = (System.currentTimeMillis() - startTime) / 1000.0;
            long recordsReturned = client.totalRecordsReturned;
            System.out.println("Query took " + delta + " seconds to return " + recordsReturned + " records");
        } catch (InterruptedException e) {
        } finally {
            if (client != null) {
                try {
                    client.closeBlocking();
                } catch (InterruptedException e) {
                }
            }
        }
    }

    private static class Client extends WebSocketClient {
        private final Gson serde = new GsonBuilder().create();
        private final Set<String> outstandingQueries = new HashSet<>();
        private final Map<String, JsonArray> records = new HashMap<>();
        private final QuerySerDe querySerDe;
        private final Query query;
        private boolean queryComplete = false;
        private long totalRecordsReturned = 0L;

        private Client(URI serverUri, Query query, QuerySerDe querySerDe) throws InterruptedException {
            super(serverUri);
            this.query = query;
            this.querySerDe = querySerDe;

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
            System.out.println("Connecting to WebSocket API at " + serverUri);
            connectBlocking();
        }

        private Map<String, String> getAwsIamAuthHeaders(URI serverUri) throws URISyntaxException {
            System.out.println("Obtaining AWS IAM creds...");
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

        public boolean isQueryComplete() {
            return queryComplete;
        }

        public long getTotalRecordsReturned() {
            return totalRecordsReturned;
        }

        @Override
        public void onOpen(ServerHandshake handshake) {
            System.out.println("Connected to WebSocket API");

            String queryJson = this.querySerDe.toJson(this.query);
            System.out.println("Submitting Query: " + queryJson);
            this.send(queryJson);
            this.outstandingQueries.add(this.query.getQueryId());
        }

        @Override
        public void onMessage(String json) {
            JsonObject message = serde.fromJson(json, JsonObject.class);
            String messageType = message.get("message").getAsString();
            String queryId = message.get("queryId").getAsString();

            if (messageType.equals("error")) {
                System.err.println("ERROR: " + message.get("error").getAsString());
                outstandingQueries.remove(queryId);

            } else if (messageType.equals("subqueries")) {
                JsonArray subQueryIdList = message.getAsJsonArray("queryIds");
                System.out.println("Query " + queryId + " split into the following subQueries:");
                for (JsonElement subQueryIdElement : subQueryIdList) {
                    String subQueryId = subQueryIdElement.getAsString();
                    System.out.println("  " + subQueryId);
                    outstandingQueries.add(subQueryId);
                }
                outstandingQueries.remove(queryId);

            } else if (messageType.equals("records")) {
                JsonArray recordBatch = message.getAsJsonArray("records");
                if (!records.containsKey(queryId)) {
                    records.put(queryId, recordBatch);
                } else {
                    records.get(queryId).addAll(recordBatch);
                }

            } else if (messageType.equals("completed")) {
                long recordCount = message.get("recordCount").getAsLong();
                boolean recordsReturnedToClient = false;
                for (JsonElement location : message.getAsJsonArray("locations")) {
                    if (location.getAsJsonObject().get("type").getAsString().equals("websocket-endpoint")) {
                        recordsReturnedToClient = true;
                    }
                }
                if (recordsReturnedToClient && recordCount > 0 && (!records.containsKey(queryId) || records.get(queryId).size() != recordCount)) {
                    System.err.println("ERROR: API said it had returned " + recordCount + " records for query " + queryId + ", but only received " + (records.containsKey(queryId) ? records.get(queryId).size() : 0));
                }
                outstandingQueries.remove(queryId);
                System.out.println(recordCount + " records returned by query: " + queryId + " Remaining pending queries: " + outstandingQueries.size());
                totalRecordsReturned += recordCount;
            } else {
                System.err.println("Received unrecognised message type: " + json);
                queryComplete = true;
            }

            if (outstandingQueries.isEmpty()) {
                queryComplete = true;
                if (records.size() > 0) {
                    System.out.println("Query results:");
                    for (Entry<String, JsonArray> subQueryRecords : records.entrySet()) {
                        for (JsonElement record : subQueryRecords.getValue()) {
                            System.out.println(record);
                        }
                    }
                }
            }
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
            queryComplete = true;
            System.out.println("Disconnected from WebSocket API: " + reason);
        }

        @Override
        public void onError(Exception error) {
            System.err.println(error);
            queryComplete = true;
        }
    }

    public static void main(String[] args) throws StateStoreException {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, args[0]);

        QueryWebSocketClient client = new QueryWebSocketClient(amazonS3, instanceProperties);
        client.run();
    }
}
