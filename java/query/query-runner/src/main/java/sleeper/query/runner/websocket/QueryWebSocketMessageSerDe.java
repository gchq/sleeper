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

    // This should stay below API Gateway's limit for web socket message payload size:
    // https://docs.aws.amazon.com/apigateway/latest/developerguide/apigateway-execution-service-websocket-limits-table.html
    public static final int DEFAULT_ROWS_PAYLOAD_SIZE = 120 * 1024;

    private final Gson gson;
    private final Gson gsonPrettyPrinting;
    private final RowJsonSerDe rowSerDe;
    private final Integer rowBatchSize;
    private final int rowsPayloadSize;

    private QueryWebSocketMessageSerDe(Schema schema, Integer rowBatchSize, int rowsPayloadSize) {
        GsonBuilder builder = new GsonBuilder()
                .registerTypeAdapter(Row.class, new RowGsonSerialiser(schema));
        this.gson = builder.create();
        this.gsonPrettyPrinting = builder.setPrettyPrinting().create();
        this.rowBatchSize = rowBatchSize;
        this.rowsPayloadSize = rowsPayloadSize;
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
        return forBatchSizeAndPayloadSize(batchSize, DEFAULT_ROWS_PAYLOAD_SIZE, schema);
    }

    public static QueryWebSocketMessageSerDe forBatchSizeAndPayloadSize(Integer batchSize, int payloadSize, Schema schema) {
        return new QueryWebSocketMessageSerDe(schema, batchSize, payloadSize);
    }

    public static QueryWebSocketMessageSerDe withNoBatchSize(Schema schema) {
        return forBatchSizeAndPayloadSize(null, DEFAULT_ROWS_PAYLOAD_SIZE, schema);
    }

    public String toJson(QueryWebSocketMessage message) {
        return gson.toJson(message);
    }

    public String toJsonPrettyPrint(QueryWebSocketMessage message) {
        return gsonPrettyPrinting.toJson(message);
    }

    public long forEachRowBatchJson(String queryId, Iterator<Row> results, Consumer<String> operation) throws QueryWebSocketRowsException {
        int baseMessageLength = toJson(QueryWebSocketMessage.rowsBatch(queryId, List.of())).length();

        List<Row> batch = new ArrayList<>();
        long count = 0;
        int remainingMessageLength = rowsPayloadSize - baseMessageLength;

        try {
            while (results.hasNext()) {
                Row row = results.next();
                batch.add(row);

                boolean batchReady = false;
                if (rowBatchSize != null && rowBatchSize > 0 && batch.size() >= rowBatchSize) {
                    batchReady = true;
                } else {
                    String rowJson = rowSerDe.toJson(row);
                    int rowJsonLength = rowJson.length() + 1; // +1 for comma that seperates rows
                    if (rowJsonLength >= remainingMessageLength) {
                        batchReady = true;
                    } else {
                        remainingMessageLength -= rowJsonLength;
                    }
                }

                if (batchReady) {
                    publishBatch(queryId, batch, operation);
                    count += batch.size();
                    batch.clear();
                    remainingMessageLength = rowsPayloadSize - baseMessageLength;
                }
            }

            if (!batch.isEmpty()) {
                publishBatch(queryId, batch, operation);
                count += batch.size();
                batch.clear();
            }
        } catch (RuntimeException e) {
            throw new QueryWebSocketRowsException(e, count);
        }
        return count;
    }

    private void publishBatch(String queryId, List<Row> rows, Consumer<String> operation) {
        LOGGER.info("Publishing batch of {} rows to WebSocket connection", rows.size());
        operation.accept(toJson(QueryWebSocketMessage.rowsBatch(queryId, rows)));
    }

    public QueryWebSocketMessage fromJson(String json) {
        try {
            return gson.fromJson(json, QueryWebSocketMessage.class).validate();
        } catch (RuntimeException e) {
            throw new QueryWebSocketMessageException(json, e);
        }
    }

}
