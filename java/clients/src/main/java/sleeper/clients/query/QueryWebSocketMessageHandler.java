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

import sleeper.clients.query.exception.WebSocketClosedException;
import sleeper.clients.query.exception.WebSocketErrorException;
import sleeper.core.row.Row;
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
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class QueryWebSocketMessageHandler {
    private final Set<String> outstandingQueries = new HashSet<>();
    private final Map<String, List<String>> parentQueryIdToSubQueryIds = new HashMap<>();
    private final Map<String, List<Row>> queryIdToRows = new TreeMap<>();
    private final QueryWebSocketMessageSerDe serDe;
    private final QuerySerDe querySerDe;
    private ClientCloser clientCloser;
    private boolean queryComplete = false;
    private boolean queryFailed = false;
    private long totalRecordsReturned = 0L;
    private CompletableFuture<List<Row>> future;
    private String currentQueryId;

    public QueryWebSocketMessageHandler(Schema schema) {
        this.serDe = QueryWebSocketMessageSerDe.withNoBatchSize(schema);
        this.querySerDe = new QuerySerDe(schema);
    }

    public void setCloser(ClientCloser clientCloser) {
        this.clientCloser = clientCloser;
    }

    public void setFuture(CompletableFuture<List<Row>> future) {
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
        QueryWebSocketMessage message;
        try {
            message = serDe.fromJson(json);
        } catch (RuntimeException e) {
            future.completeExceptionally(e);
            close();
            return;
        }

        if (message.getMessage() == QueryWebSocketMessageType.error) {
            handleError(message);
        } else if (message.getMessage() == QueryWebSocketMessageType.subqueries) {
            handleSubqueries(message);
        } else if (message.getMessage() == QueryWebSocketMessageType.rows) {
            handleRows(message);
        } else if (message.getMessage() == QueryWebSocketMessageType.completed) {
            handleCompleted(message);
        } else {
            queryFailed = true;
            future.completeExceptionally(new WebSocketErrorException("Unrecognised message type: " + message.getMessage()));
            close();
        }

        if (outstandingQueries.isEmpty()) {
            queryComplete = true;
            future.complete(getResults(currentQueryId));
            close();
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

    private void handleRows(QueryWebSocketMessage message) {
        if (!queryIdToRows.containsKey(message.getQueryId())) {
            queryIdToRows.put(message.getQueryId(), message.getRows());
        } else {
            queryIdToRows.get(message.getQueryId()).addAll(message.getRows());
        }
    }

    private void handleCompleted(QueryWebSocketMessage message) {
        long returnedRowCount = queryIdToRows.getOrDefault(message.getQueryId(), List.of()).size();
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

    public List<Row> getResults(String queryId) {
        return Stream.concat(
                Stream.of(queryId),
                parentQueryIdToSubQueryIds.getOrDefault(queryId, List.of()).stream())
                .flatMap(id -> queryIdToRows.getOrDefault(id, List.of()).stream())
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
