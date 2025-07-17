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
package sleeper.query.runner.websocket;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import sleeper.core.row.Row;

import java.util.Iterator;
import java.util.stream.Stream;

public class QueryWebSocketStatusMessageSerDe {
    private final Gson gson;
    private final Gson gsonPrettyPrinting;

    public QueryWebSocketStatusMessageSerDe() {
        GsonBuilder builder = new GsonBuilder();
        gson = builder.create();
        gsonPrettyPrinting = builder.setPrettyPrinting().create();
    }

    public String toJson(QueryWebSocketStatusMessage message) {
        return gson.toJson(message);
    }

    public String toJsonPrettyPrint(QueryWebSocketStatusMessage message) {
        return gsonPrettyPrinting.toJson(message);
    }

    public Stream<String> streamRowMessages(String queryId, Iterator<Row> rows) {
        return Stream.empty();
    }

    public QueryWebSocketStatusMessage fromJson(String json) {
        return gson.fromJson(json, QueryWebSocketStatusMessage.class);
    }

}
