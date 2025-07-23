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
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

import sleeper.clients.query.exception.MessageMalformedException;
import sleeper.clients.query.exception.MessageMissingFieldException;
import sleeper.clients.query.exception.UnknownMessageTypeException;
import sleeper.clients.query.exception.WebSocketClosedException;
import sleeper.clients.query.exception.WebSocketErrorException;
import sleeper.core.schema.Schema;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QuerySerDe;
import sleeper.query.runner.websocket.QueryWebSocketMessage;
import sleeper.query.runner.websocket.QueryWebSocketMessageSerDe;
import sleeper.query.runner.websocket.QueryWebSocketMessageType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class QueryWebSocketMessageHandler {
    private final Gson serde = new GsonBuilder().create();
    private final Set<String> outstandingQueries = new HashSet<>();
    private final Map<String, List<String>> parentQueryIdToSubQueryIds = new HashMap<>();
    private final Map<String, List<String>> subQueryIdToRows = new TreeMap<>();
    private final QueryWebSocketMessageSerDe serDe;
    private final QuerySerDe querySerDe;
    private ClientCloser clientCloser;
    private boolean queryComplete = false;
    private boolean queryFailed = false;
    private long totalRecordsReturned = 0L;
    private CompletableFuture<List<String>> future;
    private String currentQueryId;

    public QueryWebSocketMessageHandler(Schema schema) {
        this.serDe = QueryWebSocketMessageSerDe.withNoBatchSize(schema);
        this.querySerDe = new QuerySerDe(schema);
    }

    public void setCloser(ClientCloser clientCloser) {
        this.clientCloser = clientCloser;
    }

    public void setFuture(CompletableFuture<List<String>> future) {
        this.future = future;
    }

    public void onOpen(Query query, Consumer<String> messageSender) {
        QueryWebSocketClient.LOGGER.info("Connected to WebSocket API");
        String queryJson = querySerDe.toJson(query);
        QueryWebSocketClient.LOGGER.info("Submitting Query: {}", queryJson);
        messageSender.accept(queryJson);
        outstandingQueries.add(query.getQueryId());
        parentQueryIdToSubQueryIds.put(query.getQueryId(), new ArrayList<>());
        currentQueryId = query.getQueryId();
    }

    public void onMessage(String json) {
        QueryWebSocketMessage messageNew;
        try {
            messageNew = serDe.fromJson(json);
        } catch (RuntimeException e) {
            future.completeExceptionally(e);
            close();
            return;
        }
        Optional<JsonObject> messageOpt = deserialiseMessage(json);
        if (!messageOpt.isPresent()) {
            close();
            return;
        }
        JsonObject message = messageOpt.get();
        String messageType = message.get("message").getAsString();
        String queryId = message.get("queryId").getAsString();

        if (messageNew.getMessage() == QueryWebSocketMessageType.error) {
            handleError(messageNew);
        } else if (messageNew.getMessage() == QueryWebSocketMessageType.subqueries) {
            handleSubqueries(messageNew);
        } else if (messageNew.getMessage() == QueryWebSocketMessageType.rows) {
            handleRows(message, queryId);
        } else if (messageNew.getMessage() == QueryWebSocketMessageType.completed) {
            handleCompleted(messageNew);
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

    private void handleError(QueryWebSocketMessage message) {
        outstandingQueries.remove(message.getQueryId());
        queryFailed = true;
        future.completeExceptionally(new WebSocketErrorException(message.getError()));
        close();
    }

    private void handleSubqueries(QueryWebSocketMessage message) {
        QueryWebSocketClient.LOGGER.info("Query {} split into the following subQueries:", message.getQueryIds());
        for (String subQueryId : message.getQueryIds()) {
            QueryWebSocketClient.LOGGER.info("  " + subQueryId);
            outstandingQueries.add(subQueryId);
        }
        outstandingQueries.remove(message.getQueryId());
        parentQueryIdToSubQueryIds.compute(message.getQueryId(), (query, subQueries) -> {
            subQueries.addAll(message.getQueryIds());
            return subQueries;
        });
    }

    private void handleRows(JsonObject message, String queryId) {
        JsonArray recordBatch = message.getAsJsonArray("rows");
        List<String> recordList = recordBatch.asList().stream()
                .map(jsonElement -> jsonElement.getAsJsonObject())
                .map(JsonObject::toString)
                .collect(Collectors.toList());
        if (!subQueryIdToRows.containsKey(queryId)) {
            subQueryIdToRows.put(queryId, recordList);
        } else {
            subQueryIdToRows.get(queryId).addAll(recordList);
        }
    }

    private void handleCompleted(QueryWebSocketMessage message) {
        long returnedRowCount = subQueryIdToRows.getOrDefault(message.getQueryId(), List.of()).size();
        if (message.isRowsReturnedToClient() && message.getRowCount() > 0) {
            if (returnedRowCount != message.getRowCount()) {
                QueryWebSocketClient.LOGGER.error("API said it had returned {} rows for query {}, but only received {}",
                        message.getRowCount(), message.getQueryId(), returnedRowCount);
            }
        }
        outstandingQueries.remove(message.getQueryId());
        QueryWebSocketClient.LOGGER.info("{} rows returned by query {}. Remaining pending queries: {}",
                message.getRowCount(), message.getQueryId(), outstandingQueries.size());
        totalRecordsReturned += returnedRowCount;
    }

    public void onClose(String reason) {
        QueryWebSocketClient.LOGGER.info("Disconnected from WebSocket API: {}", reason);
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
        return parentQueryIdToSubQueryIds.getOrDefault(queryId, List.of()).stream()
                .flatMap(id -> subQueryIdToRows.getOrDefault(id, List.of()).stream())
                .toList();
    }

    private void close() {
        QueryWebSocketClient.LOGGER.info("Query finished, closing client");
        try {
            clientCloser.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public interface ClientCloser {
        void close() throws InterruptedException;
    }
}
