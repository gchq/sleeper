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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.query.QueryWebSocketClient.Connection;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

public class QueryWebSocketListener {
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryWebSocketListener.class);

    private final Set<String> outstandingQueryIds = new HashSet<>();
    private final List<String> subQueryIds = new ArrayList<>();
    private final Map<String, List<Row>> queryIdToRows = new TreeMap<>();
    private final QueryWebSocketMessageSerDe serDe;
    private final QuerySerDe querySerDe;
    private final Query query;
    private final QueryWebSocketHandler handler;
    private Connection connection;

    public QueryWebSocketListener(Schema schema, Query query, QueryWebSocketHandler handler) {
        this.serDe = QueryWebSocketMessageSerDe.withNoBatchSize(schema);
        this.querySerDe = new QuerySerDe(schema);
        this.query = query;
        this.handler = handler;
    }

    public void onOpen(Connection connection) {
        this.connection = connection;
        LOGGER.info("Connected to WebSocket API");
        String queryJson = querySerDe.toJson(query);
        LOGGER.info("Submitting Query: {}", queryJson);
        connection.send(queryJson);
        outstandingQueryIds.add(query.getQueryId());
    }

    public void onMessage(String json) {
        QueryWebSocketMessage message;
        try {
            message = serDe.fromJson(json);
        } catch (RuntimeException e) {
            handler.handleException(e);
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
            handler.handleException(new WebSocketErrorException("Unrecognised message type: " + message.getMessage()));
            close();
        }

        if (outstandingQueryIds.isEmpty()) {
            handler.handleResults(getResults());
            close();
        }
    }

    public void onClose(String reason) {
        LOGGER.info("Disconnected from WebSocket API: {}", reason);
        handler.handleException(new WebSocketClosedException(reason));
    }

    public void onError(Exception error) {
        handler.handleException(new WebSocketErrorException(error));
        close();
    }

    private void handleError(QueryWebSocketMessage message) {
        outstandingQueryIds.remove(message.getQueryId());
        handler.handleException(new WebSocketErrorException(message.getError()));
        close();
    }

    private void handleSubqueries(QueryWebSocketMessage message) {
        LOGGER.info("Query {} split into the following subQueries:", message.getQueryIds());
        for (String subQueryId : message.getQueryIds()) {
            LOGGER.info("  " + subQueryId);
            outstandingQueryIds.add(subQueryId);
        }
        outstandingQueryIds.remove(message.getQueryId());
        subQueryIds.addAll(message.getQueryIds());
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
                LOGGER.error("API said it had returned {} rows for query {}, but only received {}",
                        message.getRowCount(), message.getQueryId(), returnedRowCount);
            }
        }
        outstandingQueryIds.remove(message.getQueryId());
        LOGGER.info("{} rows returned by query {}. Remaining pending queries: {}",
                message.getRowCount(), message.getQueryId(), outstandingQueryIds.size());
    }

    private List<Row> getResults() {
        return Stream.concat(Stream.of(query.getQueryId()), subQueryIds.stream())
                .flatMap(id -> queryIdToRows.getOrDefault(id, List.of()).stream())
                .toList();
    }

    private void close() {
        LOGGER.info("Query finished, closing connection");
        try {
            connection.closeBlocking();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
