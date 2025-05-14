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
package sleeper.query.runnerv2.tracker;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.tracker.QueryStatusReportListener;
import sleeper.query.runnerv2.output.ApiGatewayWebSocketOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WebSocketQueryStatusReportDestination implements QueryStatusReportListener {
    private final Gson serde = new GsonBuilder().create();
    private final ApiGatewayWebSocketOutput output;
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketQueryStatusReportDestination.class);

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
        List<String> subQueryIds = subQueries.stream().map(LeafPartitionQuery::getSubQueryId).collect(Collectors.toList());
        Map<String, Object> data = new HashMap<>();
        data.put("queryIds", subQueryIds);
        this.sendStatusReport("subqueries", query.getQueryId(), data);
    }

    @Override
    public void queryCompleted(Query query, ResultsOutputInfo outputInfo) {
        queryCompleted(query.getQueryId(), outputInfo);
    }

    @Override
    public void queryCompleted(LeafPartitionQuery leafQuery, ResultsOutputInfo outputInfo) {
        queryCompleted(leafQuery.getQueryId(), outputInfo);
    }

    private void queryCompleted(String queryId, ResultsOutputInfo outputInfo) {
        String message = outputInfo.getError() == null ? "completed" : "error";

        Map<String, Object> data = new HashMap<>();
        data.put("recordCount", outputInfo.getRecordCount());
        data.put("locations", outputInfo.getLocations());
        if (outputInfo.getError() != null) {
            data.put("error", outputInfo.getError().getClass().getSimpleName() + ": " + outputInfo.getError().getMessage());
        }

        sendStatusReport(message, queryId, data);
    }

    @Override
    public void queryFailed(Query query, Exception e) {
        queryFailed(query.getQueryId(), e);
    }

    @Override
    public void queryFailed(String queryId, Exception e) {
        Map<String, Object> data = new HashMap<>();
        data.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
        sendStatusReport("error", queryId, data);
    }

    @Override
    public void queryFailed(LeafPartitionQuery leafQuery, Exception e) {
        queryFailed(leafQuery.getQueryId(), e);
    }

    private void sendStatusReport(String message, String queryId, Map<String, Object> data) {
        HashMap<String, Object> record = new HashMap<>(data);
        record.put("message", message);
        record.put("queryId", queryId);

        try {
            output.sendString(serde.toJson(record));
        } catch (IOException e) {
            LOGGER.error("Unable to send query status report to websocket", e);
        }
    }
}
