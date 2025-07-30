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
package sleeper.query.runner.tracker;

import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.tracker.QueryStatusReportListener;
import sleeper.query.runner.websocket.ApiGatewayWebSocketOutput;
import sleeper.query.runner.websocket.QueryWebSocketMessage;
import sleeper.query.runner.websocket.QueryWebSocketMessageSerDe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

public class WebSocketQueryStatusReportDestination implements QueryStatusReportListener {
    private final QueryWebSocketMessageSerDe serDe = QueryWebSocketMessageSerDe.forStatusMessages();
    private final ApiGatewayWebSocketOutput output;

    public WebSocketQueryStatusReportDestination(ApiGatewayWebSocketOutput output) {
        this.output = output;
    }

    public WebSocketQueryStatusReportDestination(Map<String, String> config) {
        this.output = ApiGatewayWebSocketOutput.fromConfig(config);
    }

    @Override
    public void queryQueued(Query query) {
        // Ignore
    }

    @Override
    public void queryInProgress(Query query) {
        // Ignore
    }

    @Override
    public void queryInProgress(LeafPartitionQuery leafQuery) {
        // Ignore
    }

    @Override
    public void subQueriesCreated(Query query, List<LeafPartitionQuery> subQueries) {
        List<String> subQueryIds = subQueries.stream().map(LeafPartitionQuery::getSubQueryId).toList();
        send(QueryWebSocketMessage.queryWasSplitToSubqueries(query.getQueryId(), subQueryIds));
    }

    @Override
    public void queryCompleted(Query query, ResultsOutputInfo outputInfo) {
        queryCompleted(query.getQueryId(), outputInfo);
    }

    @Override
    public void queryCompleted(LeafPartitionQuery leafQuery, ResultsOutputInfo outputInfo) {
        queryCompleted(leafQuery.getSubQueryId(), outputInfo);
    }

    private void queryCompleted(String queryId, ResultsOutputInfo outputInfo) {
        if (outputInfo.getError() != null) {
            String error = outputInfo.getError().getClass().getSimpleName() + ": " + outputInfo.getError().getMessage();
            send(QueryWebSocketMessage.queryError(queryId, error, outputInfo.getRowCount(), outputInfo.getLocations()));
        } else {
            send(QueryWebSocketMessage.queryCompleted(queryId, outputInfo.getRowCount(), outputInfo.getLocations()));
        }
    }

    @Override
    public void queryFailed(Query query, Exception e) {
        queryFailed(query.getQueryId(), e);
    }

    @Override
    public void queryFailed(String queryId, Exception e) {
        String error = e.getClass().getSimpleName() + ": " + e.getMessage();
        send(QueryWebSocketMessage.queryError(queryId, error));
    }

    @Override
    public void queryFailed(LeafPartitionQuery leafQuery, Exception e) {
        queryFailed(leafQuery.getSubQueryId(), e);
    }

    private void send(QueryWebSocketMessage message) {
        try {
            output.sendString(serDe.toJson(message));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
