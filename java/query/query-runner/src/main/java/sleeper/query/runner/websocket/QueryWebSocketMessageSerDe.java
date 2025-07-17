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
import sleeper.core.row.serialiser.RowJsonSerDe.RowGsonSerialiser;
import sleeper.core.schema.Schema;

import java.util.Iterator;
import java.util.stream.Stream;

public class QueryWebSocketMessageSerDe {
    private final Gson gson;
    private final Gson gsonPrettyPrinting;
    private final Schema schema;
    private final long recordBatchSize;
    private final long recordPayloadSize;

    private QueryWebSocketMessageSerDe(Schema schema, long recordBatchSize, long recordPayloadSize) {
        GsonBuilder builder = new GsonBuilder()
                .registerTypeAdapter(Row.class, new RowGsonSerialiser(schema));
        gson = builder.create();
        gsonPrettyPrinting = builder.setPrettyPrinting().create();
        this.schema = schema;
        this.recordBatchSize = recordBatchSize;
        this.recordPayloadSize = recordPayloadSize;
    }

    public static QueryWebSocketMessageSerDe forStatusMessages() {
        return new QueryWebSocketMessageSerDe(null, 0, 0);
    }

    public static QueryWebSocketMessageSerDe forBatchSizeAndPayloadSize(long batchSize, long payloadSize, Schema schema) {
        return new QueryWebSocketMessageSerDe(schema, batchSize, payloadSize);
    }

    public String toJson(QueryWebSocketMessage message) {
        return gson.toJson(message);
    }

    public String toJsonPrettyPrint(QueryWebSocketMessage message) {
        return gsonPrettyPrinting.toJson(message);
    }

    public Stream<String> streamRowBatchesJson(String queryId, Iterator<Row> rows) {
        return Stream.empty();
    }

    public QueryWebSocketMessage fromJson(String json) {
        return gson.fromJson(json, QueryWebSocketMessage.class);
    }

}
