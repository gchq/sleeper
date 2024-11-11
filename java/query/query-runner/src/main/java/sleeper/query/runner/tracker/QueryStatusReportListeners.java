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
package sleeper.query.runner.tracker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.tracker.QueryStatusReportListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryStatusReportListeners implements QueryStatusReportListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryStatusReportListeners.class);

    private List<QueryStatusReportListener> listeners = new ArrayList<>();

    private QueryStatusReportListeners() {
    }

    private QueryStatusReportListeners(List<QueryStatusReportListener> listeners) {
        if (listeners != null) {
            this.listeners = listeners;
        }
    }

    public static QueryStatusReportListeners fromConfig(List<Map<String, String>> destinationsConfig) {
        if (destinationsConfig == null || destinationsConfig.isEmpty()) {
            return new QueryStatusReportListeners();
        }

        List<QueryStatusReportListener> listeners = destinationsConfig.stream()
                .map(QueryStatusReportListeners::listenerFromConfig)
                .collect(Collectors.toList());
        return new QueryStatusReportListeners(listeners);
    }

    private static QueryStatusReportListener listenerFromConfig(Map<String, String> destinationConfig) {
        if (!destinationConfig.containsKey(QueryStatusReportListener.DESTINATION)) {
            throw new IllegalArgumentException(QueryStatusReportListener.class.getSimpleName() + " config: " + destinationConfig + " is missing attribute: " + QueryStatusReportListener.DESTINATION);
        }

        String destination = destinationConfig.get(QueryStatusReportListener.DESTINATION);
        if (destination.equals(WebSocketQueryStatusReportDestination.DESTINATION_NAME)) {
            return new WebSocketQueryStatusReportDestination(destinationConfig);
        } else if (destination.equals(DynamoDBQueryTracker.DESTINATION)) {
            return new DynamoDBQueryTracker(destinationConfig);
        }

        throw new IllegalArgumentException("Unrecognised " + QueryStatusReportListener.class.getSimpleName() + " " + QueryStatusReportListener.DESTINATION + ": " + destination);
    }

    public void add(QueryStatusReportListener listener) {
        listeners.add(listener);
    }

    @Override
    public void queryQueued(Query query) {
        LOGGER.info("Query Queued: {}", query);
        listeners.forEach(listener -> listener.queryQueued(query));
    }

    @Override
    public void queryInProgress(Query query) {
        LOGGER.info("Query InProgress: {}", query);
        listeners.forEach(listener -> listener.queryInProgress(query));
    }

    @Override
    public void queryInProgress(LeafPartitionQuery leafQuery) {
        LOGGER.info("Query InProgress: {}", leafQuery);
        listeners.forEach(listener -> listener.queryInProgress(leafQuery));
    }

    @Override
    public void subQueriesCreated(Query query, List<LeafPartitionQuery> subQueries) {
        LOGGER.info("SubQueries Created: {}", subQueries);
        listeners.forEach(listener -> listener.subQueriesCreated(query, subQueries));
    }

    @Override
    public void queryCompleted(Query query, ResultsOutputInfo outputInfo) {
        LOGGER.info("Query Completed: {} {}", query, outputInfo);
        listeners.forEach(listener -> listener.queryCompleted(query, outputInfo));
    }

    @Override
    public void queryCompleted(LeafPartitionQuery leafQuery, ResultsOutputInfo outputInfo) {
        LOGGER.info("Query Completed: {} {}", leafQuery, outputInfo);
        listeners.forEach(listener -> listener.queryCompleted(leafQuery, outputInfo));
    }

    @Override
    public void queryFailed(Query query, Exception e) {
        LOGGER.error("Query Failed: {}", query, e);
        listeners.forEach(listener -> listener.queryFailed(query, e));
    }

    @Override
    public void queryFailed(String queryId, Exception e) {
        LOGGER.error("Query Failed: {}", queryId, e);
        listeners.forEach(listener -> listener.queryFailed(queryId, e));
    }

    @Override
    public void queryFailed(LeafPartitionQuery leafQuery, Exception e) {
        LOGGER.error("Query Failed: {}", leafQuery, e);
        listeners.forEach(listener -> listener.queryFailed(leafQuery, e));
    }
}
