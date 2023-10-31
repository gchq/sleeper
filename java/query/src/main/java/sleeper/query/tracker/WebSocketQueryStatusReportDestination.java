/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.query.tracker;

import com.amazonaws.auth.AWSCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.query.model.Query;
import sleeper.query.model.LeafPartitionQuery;
import sleeper.query.model.output.ResultsOutputInfo;
import sleeper.query.model.output.WebSocketOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WebSocketQueryStatusReportDestination extends WebSocketOutput implements QueryStatusReportListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketQueryStatusReportDestination.class);

    public WebSocketQueryStatusReportDestination(String region, String endpoint, String connectionId) {
        super(region, endpoint, connectionId);
    }

    public WebSocketQueryStatusReportDestination(String awsRegion, String endpoint, String connectionId, AWSCredentials awsCredentials) {
        super(awsRegion, endpoint, connectionId, awsCredentials);
    }

    public WebSocketQueryStatusReportDestination(Map<String, String> config) {
        super(config);
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
    public void queryInProgress(LeafPartitionQuery query) {
        // Ignore
    }

    @Override
    public void subQueriesCreated(Query query, List<LeafPartitionQuery> subQueries) {
        List<String> subQueryIds = subQueries.stream().map(LeafPartitionQuery::getSubQueryId).collect(Collectors.toList());
        Map<String, Object> data = new HashMap<>();
        data.put("queryIds", subQueryIds);
        this.sendStatusReport("subqueries", getQueryId(query), data);
    }

    @Override
    public void queryCompleted(Query query, ResultsOutputInfo outputInfo) {
        queryCompleted(getQueryId(query), outputInfo);
    }

    @Override
    public void queryCompleted(LeafPartitionQuery query, ResultsOutputInfo outputInfo) {
        queryCompleted(getQueryId(query), outputInfo);
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
        Map<String, Object> data = new HashMap<>();
        data.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
        sendStatusReport("error", getQueryId(query), data);
    }

    @Override
    public void queryFailed(LeafPartitionQuery query, Exception e) {
        Map<String, Object> data = new HashMap<>();
        data.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
        sendStatusReport("error", getQueryId(query), data);
    }

    private void sendStatusReport(String message, String queryId, Map<String, Object> data) {
        HashMap<String, Object> record = new HashMap<>(data);
        record.put("message", message);
        record.put("queryId", queryId);

        try {
            sendJson(record);
        } catch (IOException e) {
            LOGGER.error("Unable to send query status report to websocket", e);
        }
    }
}
