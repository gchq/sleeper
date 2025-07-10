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

import sleeper.clients.query.FakeWebSocketClient.WebSocketResponse;
import sleeper.core.row.Row;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryWebSocketClientTestHelper {
    private static final Gson GSON = new GsonBuilder().create();

    private QueryWebSocketClientTestHelper() {
    }

    public static String createdSubQueries(String queryId, String... subQueryIds) {
        return "{" +
                "\"queryId\":\"" + queryId + "\", " +
                "\"message\":\"subqueries\"," +
                "\"queryIds\":[" + Stream.of(subQueryIds).map(id -> "\"" + id + "\"").collect(Collectors.joining(",")) + "]" +
                "}";
    }

    public static String errorMessage(String queryId, String message) {
        return "{" +
                "\"queryId\":\"" + queryId + "\", " +
                "\"message\":\"error\"," +
                "\"error\":\"" + message + "\"" +
                "}";
    }

    public static String unknownMessage(String queryId) {
        return "{" +
                "\"queryId\":\"" + queryId + "\"," +
                "\"message\":\"unknown\"" +
                "}";
    }

    public static String queryResult(String queryId, Row... rows) {
        return "{" +
                "\"queryId\":\"" + queryId + "\", " +
                "\"message\":\"rows\"," +
                "\"rows\":" + GSON.toJson(List.of(rows)) +
                "}";
    }

    public static String completedQuery(String queryId, long rowCount) {
        return "{" +
                "\"queryId\":\"" + queryId + "\", " +
                "\"message\":\"completed\"," +
                "\"rowCount\":\"" + rowCount + "\"," +
                "\"locations\":[{\"type\":\"websocket-endpoint\"}]" +
                "}";
    }

    public static WebSocketResponse message(String message) {
        return client -> client.onMessage(message);
    }

    public static WebSocketResponse close(String reason) {
        return client -> client.onClose(reason);
    }

    public static WebSocketResponse error(Exception error) {
        return client -> client.onError(error);
    }

    public static String asJson(Row row) {
        return GSON.toJson(row);
    }
}
