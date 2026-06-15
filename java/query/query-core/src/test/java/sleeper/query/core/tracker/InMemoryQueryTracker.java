/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.query.core.tracker;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.output.ResultsOutputInfo;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.QueryProperty.QUERY_TRACKER_ITEM_TTL_IN_DAYS;

/**
 * An in-memory implementation of the query tracker. Tracks and reports on the status of queries.
 */
public class InMemoryQueryTracker implements QueryStatusReportListener, QueryTrackerStore {

    private final InstanceProperties instanceProperties;
    private final Supplier<Instant> timeSupplier;
    private final Map<String, TrackedQuery> queryIdToStatus = new HashMap<>();
    private final Map<String, TrackedQuery> subQueryIdToStatus = new HashMap<>();

    public InMemoryQueryTracker(InstanceProperties instanceProperties) {
        this(instanceProperties, Instant::now);
    }

    public InMemoryQueryTracker(InstanceProperties instanceProperties, Supplier<Instant> timeSupplier) {
        this.instanceProperties = instanceProperties;
        this.timeSupplier = timeSupplier;
    }

    @Override
    public TrackedQuery getStatus(String queryId) throws QueryTrackerException {
        TrackedQuery status = queryIdToStatus.get(queryId);
        if (status == null) {
            throw new RuntimeException("Query ID was not found: " + queryId);
        }
        return status;
    }

    @Override
    public TrackedQuery getStatus(String queryId, String subQueryId) throws QueryTrackerException {
        TrackedQuery status = subQueryIdToStatus.get(subQueryId);
        if (status == null) {
            throw new RuntimeException("Sub-query ID was not found: " + subQueryId);
        }
        if (!Objects.equals(status.getQueryId(), queryId)) {
            throw new RuntimeException("Sub-query ID was found but query ID was not found: " + queryId);
        }
        return status;
    }

    @Override
    public List<TrackedQuery> getAllQueries() {
        return streamAllQueries().toList();
    }

    /**
     * Streams through the status of all queries and sub-queries.
     *
     * @return the query statuses
     */
    public Stream<TrackedQuery> streamAllQueries() {
        return Stream.concat(queryIdToStatus.values().stream(), subQueryIdToStatus.values().stream());
    }

    @Override
    public List<TrackedQuery> getQueriesWithState(QueryState state) {
        return streamAllQueries().filter(query -> Objects.equals(state, query.getLastKnownState())).toList();
    }

    @Override
    public List<TrackedQuery> getFailedQueries() {
        return streamAllQueries()
                .filter(query -> query.getLastKnownState() == QueryState.FAILED
                        || query.getLastKnownState() == QueryState.PARTIALLY_FAILED)
                .toList();
    }

    @Override
    public void queryQueued(Query query) {
        queryIdToStatus.put(query.getQueryId(), newQuery(query, QueryState.QUEUED));
    }

    @Override
    public void queryInProgress(Query query) {
        upsertQuery(query,
                () -> newQuery(query, QueryState.IN_PROGRESS),
                status -> updateQuery(status).lastKnownState(QueryState.IN_PROGRESS).build());
    }

    @Override
    public void queryInProgress(LeafPartitionQuery leafQuery) {
        upsertQuery(leafQuery,
                () -> newQuery(leafQuery, QueryState.IN_PROGRESS),
                status -> updateQuery(status).lastKnownState(QueryState.IN_PROGRESS).build());
    }

    @Override
    public void subQueriesCreated(Query query, List<LeafPartitionQuery> subQueries) {
        for (LeafPartitionQuery subQuery : subQueries) {
            subQueryIdToStatus.put(subQuery.getSubQueryId(), newQuery(subQuery, QueryState.QUEUED));
        }
    }

    @Override
    public void queryCompleted(Query query, ResultsOutputInfo outputInfo) {
        upsertQuery(query,
                () -> newQueryBuilder(query, QueryState.COMPLETED).outputInfo(outputInfo).build(),
                status -> updateQuery(status).lastKnownState(QueryState.COMPLETED).outputInfo(outputInfo).build());
    }

    @Override
    public void queryCompleted(LeafPartitionQuery leafQuery, ResultsOutputInfo outputInfo) {
        upsertQuery(leafQuery,
                () -> newQueryBuilder(leafQuery, QueryState.COMPLETED).outputInfo(outputInfo).build(),
                status -> updateQuery(status).lastKnownState(QueryState.COMPLETED).outputInfo(outputInfo).build());
    }

    @Override
    public void queryFailed(Query query, Exception e) {
        upsertQuery(query,
                () -> newQueryBuilder(query, QueryState.FAILED).error(e).build(),
                status -> updateQuery(status).lastKnownState(QueryState.FAILED).error(e).build());
    }

    @Override
    public void queryFailed(String queryId, Exception e) {
        upsertQuery(queryId,
                () -> newQueryBuilder(queryId, QueryState.FAILED).error(e).build(),
                status -> updateQuery(status).lastKnownState(QueryState.FAILED).error(e).build());
    }

    @Override
    public void queryFailed(LeafPartitionQuery leafQuery, Exception e) {
        upsertQuery(leafQuery,
                () -> newQueryBuilder(leafQuery, QueryState.FAILED).error(e).build(),
                status -> updateQuery(status).lastKnownState(QueryState.FAILED).error(e).build());
    }

    private void upsertQuery(Query query, Supplier<TrackedQuery> createNew, Function<TrackedQuery, TrackedQuery> update) {
        upsertQuery(query.getQueryId(), createNew, update);
    }

    private void upsertQuery(String queryId, Supplier<TrackedQuery> createNew, Function<TrackedQuery, TrackedQuery> update) {
        queryIdToStatus.put(queryId,
                Optional.ofNullable(queryIdToStatus.get(queryId))
                        .map(update)
                        .orElseGet(createNew));
    }

    private void upsertQuery(LeafPartitionQuery query, Supplier<TrackedQuery> createNew, Function<TrackedQuery, TrackedQuery> update) {
        subQueryIdToStatus.put(query.getSubQueryId(),
                Optional.ofNullable(subQueryIdToStatus.get(query.getSubQueryId()))
                        .map(update)
                        .orElseGet(createNew));
    }

    private TrackedQuery newQuery(Query query, QueryState state) {
        return newQueryBuilder(query, state).build();
    }

    private TrackedQuery newQuery(LeafPartitionQuery leafQuery, QueryState state) {
        return newQueryBuilder(leafQuery, state).build();
    }

    private TrackedQuery.Builder newQueryBuilder(Query query, QueryState state) {
        return newQueryBuilder(query.getQueryId(), state);
    }

    private TrackedQuery.Builder newQueryBuilder(LeafPartitionQuery leafQuery, QueryState state) {
        return newQueryBuilder(leafQuery.getQueryId(), leafQuery.getSubQueryId(), state);
    }

    private TrackedQuery.Builder newQueryBuilder(String queryId, QueryState state) {
        return newQueryBuilder(queryId, null, state);
    }

    private TrackedQuery.Builder newQueryBuilder(String queryId, String subQueryId, QueryState state) {
        Instant now = timeSupplier.get();
        return TrackedQuery.builder()
                .queryId(queryId)
                .subQueryId(subQueryId)
                .lastUpdateTime(now)
                .expiryDate(now.plus(queryTtl()))
                .lastKnownState(state);
    }

    private TrackedQuery.Builder updateQuery(TrackedQuery status) {
        Instant now = timeSupplier.get();
        return status.toBuilder()
                .lastUpdateTime(now)
                .expiryDate(now.plus(queryTtl()));
    }

    private Duration queryTtl() {
        return Duration.ofDays(instanceProperties.getLong(QUERY_TRACKER_ITEM_TTL_IN_DAYS));
    }

}
