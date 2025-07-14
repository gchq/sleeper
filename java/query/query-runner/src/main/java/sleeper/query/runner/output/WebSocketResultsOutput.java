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
package sleeper.query.runner.output;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.row.Row;
import sleeper.core.row.serialiser.RowJsonSerDe;
import sleeper.core.schema.Schema;
import sleeper.query.core.model.QueryOrLeafPartitionQuery;
import sleeper.query.core.output.ResultsOutput;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.output.ResultsOutputLocation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A query results output to write results to a client connected via a WebSocket API Gateway.
 */
public class WebSocketResultsOutput implements ResultsOutput {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketResultsOutput.class);
    public static final int MAX_PAYLOAD_SIZE = 128 * 1024;

    private final Gson serde;
    private final ApiGatewayWebSocketOutput output;
    private final List<ResultsOutputLocation> outputLocations = new ArrayList<>();
    private final Long maxBatchSize;

    public WebSocketResultsOutput(Schema schema, Map<String, String> config) {
        this.serde = new GsonBuilder()
                .registerTypeAdapter(Row.class, new RowJsonSerDe.RowGsonSerialiser(schema))
                .create();
        this.output = ApiGatewayWebSocketOutput.fromConfig(config);
        String maxBatchSize = config.get(WebSocketOutput.MAX_BATCH_SIZE);
        this.maxBatchSize = maxBatchSize != null && !maxBatchSize.isEmpty() ? Long.parseLong(maxBatchSize) : null;
        this.outputLocations.add(new ResultsOutputLocation("websocket-endpoint", config.get(WebSocketOutput.ENDPOINT)));
        this.outputLocations.add(new ResultsOutputLocation("websocket-connection-id", config.get(WebSocketOutput.CONNECTION_ID)));
    }

    @Override
    public ResultsOutputInfo publish(QueryOrLeafPartitionQuery query, CloseableIterator<Row> results) {
        String queryId = query.getQueryId();

        Map<String, Object> message = new HashMap<>();
        message.put("message", "rows");
        message.put("queryId", queryId);
        message.put("rows", Collections.emptyList());
        int baseMessageLength = serde.toJson(message).length();

        List<Row> batch = new ArrayList<>();
        long count = 0;
        int remainingMessageLength = MAX_PAYLOAD_SIZE - baseMessageLength;

        try {
            while (results.hasNext()) {
                Row row = results.next();

                boolean batchReady = false;
                if (maxBatchSize != null && maxBatchSize > 0 && batch.size() >= maxBatchSize) {
                    batchReady = true;
                } else {
                    String rowJson = serde.toJson(row);
                    int rowJsonLength = rowJson.length() + 1; // +1 for comma that seperates rows
                    if (rowJsonLength >= remainingMessageLength) {
                        batchReady = true;
                        remainingMessageLength = MAX_PAYLOAD_SIZE - baseMessageLength - rowJsonLength;
                    } else {
                        remainingMessageLength -= rowJsonLength;
                    }
                }

                if (batchReady) {
                    publishBatch(message, batch);
                    count += batch.size();
                    batch.clear();
                }

                batch.add(row);
            }

            if (!batch.isEmpty()) {
                publishBatch(message, batch);
                count += batch.size();
                batch.clear();
            }
        } catch (Exception e) {
            LOGGER.error("Error publishing results to WebSocket", e);
            return new ResultsOutputInfo(count, outputLocations, e);
        } finally {
            try {
                results.close();
            } catch (Exception e) {
                LOGGER.error("Exception closing results of query", e);
            }
        }

        return new ResultsOutputInfo(count, outputLocations);
    }

    private void publishBatch(Map<String, Object> message, List<Row> rows) throws IOException {
        LOGGER.info("Publishing batch of {} rows to WebSocket connection", rows.size());
        message.put("rows", rows);
        output.sendString(serde.toJson(message));
    }
}
