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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import sleeper.clients.FakeWebSocketClient.WebSocketResponse;
import sleeper.core.record.Record;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryWebSocketClientTestHelper {

    private static final Gson GSON = new GsonBuilder().create();

    private QueryWebSocketClientTestHelper() {
    }

    public static String queryResult(String queryId, Record... records) {
        return "{" +
                "\"queryId\":\"" + queryId + "\", " +
                "\"message\":\"records\"," +
                "\"records\":[" + Stream.of(records).map(GSON::toJson).collect(Collectors.joining(",")) + "]" +
                "}";
    }

    public static String completedQuery(String queryId, long recordCount) {
        return "{" +
                "\"queryId\":\"" + queryId + "\", " +
                "\"message\":\"completed\"," +
                "\"recordCount\":\"" + recordCount + "\"," +
                "\"locations\":[{\"type\":\"websocket-endpoint\"}]" +
                "}";
    }

    public static WebSocketResponse message(String message) {
        return messageHandler -> messageHandler.onMessage(message);
    }
}
