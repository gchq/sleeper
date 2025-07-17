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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.row.Row;
import sleeper.core.row.serialiser.RowJsonSerDe;
import sleeper.core.row.serialiser.RowJsonSerDe.RowGsonSerialiser;
import sleeper.core.schema.Schema;
import sleeper.query.runner.output.WebSocketOutput;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class QueryWebSocketMessageSerDe {
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryWebSocketMessageSerDe.class);
    public static final int DEFAULT_RECORD_PAYLOAD_SIZE = 128 * 1024;

    private final Gson gson;
    private final Gson gsonPrettyPrinting;
    private final RowJsonSerDe rowSerDe;
    private final Integer recordBatchSize;
    private final int recordPayloadSize;

    private QueryWebSocketMessageSerDe(Schema schema, Integer recordBatchSize, int recordPayloadSize) {
        GsonBuilder builder = new GsonBuilder()
                .registerTypeAdapter(Row.class, new RowGsonSerialiser(schema));
        this.gson = builder.create();
        this.gsonPrettyPrinting = builder.setPrettyPrinting().create();
        this.recordBatchSize = recordBatchSize;
        this.recordPayloadSize = recordPayloadSize;
        if (schema != null) {
            rowSerDe = new RowJsonSerDe(schema);
        } else {
            rowSerDe = null;
        }
    }

    public static QueryWebSocketMessageSerDe forStatusMessages() {
        return new QueryWebSocketMessageSerDe(null, null, 0);
    }

    public static QueryWebSocketMessageSerDe fromConfig(Schema schema, Map<String, String> config) {
        String batchSizeStr = config.get(WebSocketOutput.MAX_BATCH_SIZE);
        Integer batchSize = batchSizeStr != null && !batchSizeStr.isEmpty() ? Integer.parseInt(batchSizeStr) : null;
        return forBatchSizeAndPayloadSize(batchSize, DEFAULT_RECORD_PAYLOAD_SIZE, schema);
    }

    public static QueryWebSocketMessageSerDe forBatchSizeAndPayloadSize(Integer batchSize, int payloadSize, Schema schema) {
        return new QueryWebSocketMessageSerDe(schema, batchSize, payloadSize);
    }

    public String toJson(QueryWebSocketMessage message) {
        return gson.toJson(message);
    }

    public String toJsonPrettyPrint(QueryWebSocketMessage message) {
        return gsonPrettyPrinting.toJson(message);
    }

    public void forEachRowBatchJson(String queryId, Iterator<Row> results, Consumer<String> operation) throws QueryRowBatchException {
        int baseMessageLength = toJson(QueryWebSocketMessage.rowsBatch(queryId, List.of())).length();

        List<Row> batch = new ArrayList<>();
        long count = 0;
        int remainingMessageLength = recordPayloadSize - baseMessageLength;

        try {
            while (results.hasNext()) {
                Row row = results.next();
                batch.add(row);

                boolean batchReady = false;
                if (recordBatchSize != null && recordBatchSize > 0 && batch.size() >= recordBatchSize) {
                    batchReady = true;
                } else {
                    String rowJson = rowSerDe.toJson(row);
                    int rowJsonLength = rowJson.length() + 1; // +1 for comma that seperates rows
                    if (rowJsonLength >= remainingMessageLength) {
                        batchReady = true;
                        remainingMessageLength = recordPayloadSize - baseMessageLength - rowJsonLength;
                    } else {
                        remainingMessageLength -= rowJsonLength;
                    }
                }

                if (batchReady) {
                    operation.accept(toJson(QueryWebSocketMessage.rowsBatch(queryId, batch)));
                    count += batch.size();
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                operation.accept(toJson(QueryWebSocketMessage.rowsBatch(queryId, batch)));
                count += batch.size();
                batch.clear();
            }
        } catch (Exception e) {
            throw new QueryRowBatchException(e, count);
        }
    }

    public QueryWebSocketMessage fromJson(String json) {
        return gson.fromJson(json, QueryWebSocketMessage.class);
    }

}
