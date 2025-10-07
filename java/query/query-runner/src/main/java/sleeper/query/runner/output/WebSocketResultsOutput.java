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

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.query.core.model.QueryOrLeafPartitionQuery;
import sleeper.query.core.output.ResultsOutput;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.output.ResultsOutputLocation;
import sleeper.query.runner.websocket.ApiGatewayWebSocketOutput;
import sleeper.query.runner.websocket.QueryWebSocketMessageSerDe;
import sleeper.query.runner.websocket.QueryWebSocketRowsException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A query results output to write results to a client connected via a WebSocket API Gateway.
 */
public class WebSocketResultsOutput implements ResultsOutput {
    public static final int MAX_PAYLOAD_SIZE = 128 * 1024;

    private final QueryWebSocketMessageSerDe serDe;
    private final ApiGatewayWebSocketOutput output;
    private final List<ResultsOutputLocation> outputLocations = new ArrayList<>();

    public WebSocketResultsOutput(Schema schema, Map<String, String> config) {
        this.serDe = QueryWebSocketMessageSerDe.fromConfig(schema, config);
        this.output = ApiGatewayWebSocketOutput.fromConfig(config);
        this.outputLocations.add(new ResultsOutputLocation("websocket-endpoint", config.get(WebSocketOutput.ENDPOINT)));
        this.outputLocations.add(new ResultsOutputLocation("websocket-connection-id", config.get(WebSocketOutput.CONNECTION_ID)));
    }

    @Override
    public ResultsOutputInfo publish(QueryOrLeafPartitionQuery query, CloseableIterator<Row> results) {
        String queryId = query.getQueryId();
        try {
            long rowsSent = serDe.forEachRowBatchJson(queryId, results, json -> {
                try {
                    output.sendString(json);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            return new ResultsOutputInfo(rowsSent, outputLocations);
        } catch (QueryWebSocketRowsException e) {
            return new ResultsOutputInfo(e.getRowsSent(), outputLocations, e.getCause());
        } finally {
            try {
                results.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
