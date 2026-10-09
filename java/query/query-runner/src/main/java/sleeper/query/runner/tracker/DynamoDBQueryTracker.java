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
package sleeper.query.runner.tracker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.Update;
import software.amazon.awssdk.services.dynamodb.paginators.QueryIterable;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ExponentialBackoffWithJitter.WaitRange;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.tracker.QueryState;
import sleeper.query.core.tracker.QueryStatusReportListener;
import sleeper.query.core.tracker.QueryTrackerException;
import sleeper.query.core.tracker.QueryTrackerStore;
import sleeper.query.core.tracker.TrackedQuery;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_TRACKER_TABLE_NAME;
import static sleeper.core.properties.instance.QueryProperty.QUERY_TRACKER_ITEM_TTL_IN_DAYS;

/**
 * The query tracker updates and keeps track of the status of queries so that clients
 * can see how complete it is or if part or all of the query failed.
 */
public class DynamoDBQueryTracker implements QueryStatusReportListener, QueryTrackerStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBQueryTracker.class);

    public static final String DESTINATION = "DYNAMODB";
    public static final WaitRange TRANSACTION_CONFLICT_WAIT_RANGE = WaitRange.firstAndMaxWaitCeilingSecs(1, 20);
    private static final int MAX_ATTEMPTS_PER_TRANSACTION = 10;
    // 32 shards was chosen based on experimentation: a query with 16384 subqueries executed via lambda results in
    // around 400 retries with a slightly smaller number of subqueries affected, and a maximum number of
    // attempts of either 2 or 3. This could be increased in future, at the expense of more costly queries to check
    // if the parent query has completed. As the shards are updated in a transaction, this figure must be <= 99.
    public static final int COUNTER_SHARDS = 32;
    private static final Set<String> RETRYABLE_CANCELLATION_CODES = Set.of(
            "TransactionConflict", "ProvisionedThroughputExceeded", "ThrottlingError");
    public static final String NON_NESTED_QUERY_PLACEHOLDER = DynamoDBQueryTrackerEntry.NON_NESTED_QUERY_PLACEHOLDER;
    public static final String QUERY_ID = DynamoDBQueryTrackerEntry.QUERY_ID;
    public static final String SUB_QUERY_ID = DynamoDBQueryTrackerEntry.SUB_QUERY_ID;
    public static final String LAST_KNOWN_STATE = DynamoDBQueryTrackerEntry.LAST_KNOWN_STATE;

    private final DynamoDbClient dynamoClient;
    private final String trackerTableName;
    private final long queryTrackerTTL;
    private final ExponentialBackoffWithJitter transactionConflictBackoff;

    public DynamoDBQueryTracker(InstanceProperties instanceProperties, DynamoDbClient dynamoClient) {
        this(instanceProperties, dynamoClient, new ExponentialBackoffWithJitter(TRANSACTION_CONFLICT_WAIT_RANGE));
    }

    public DynamoDBQueryTracker(InstanceProperties instanceProperties, DynamoDbClient dynamoClient, ExponentialBackoffWithJitter transactionConflictBackoff) {
        this.trackerTableName = instanceProperties.get(QUERY_TRACKER_TABLE_NAME);
        this.queryTrackerTTL = instanceProperties.getLong(QUERY_TRACKER_ITEM_TTL_IN_DAYS);
        this.dynamoClient = dynamoClient;
        this.transactionConflictBackoff = transactionConflictBackoff;
    }

    public DynamoDBQueryTracker(Map<String, String> destinationConfig) {
        this.trackerTableName = destinationConfig.get(QUERY_TRACKER_TABLE_NAME.getPropertyName());
        String ttl = destinationConfig.get(QUERY_TRACKER_ITEM_TTL_IN_DAYS.getPropertyName());
        this.queryTrackerTTL = Long.parseLong(ttl != null ? ttl : QUERY_TRACKER_ITEM_TTL_IN_DAYS.getDefaultValue());
        this.dynamoClient = DynamoDbClient.create();
        this.transactionConflictBackoff = new ExponentialBackoffWithJitter(TRANSACTION_CONFLICT_WAIT_RANGE);
    }

    @Override
    public TrackedQuery getStatus(String queryId) throws QueryTrackerException {
        // Don't use a consistent read here because this is for status reporting purposes only.
        ParentQueryState state = readParentQueryState(queryId, false);
        if (state.parentItem == null) {
            return null;
        }
        TrackedQuery parent = DynamoDBQueryTrackerEntry.toTrackedQuery(state.parentItem);
        if (!state.countersFound) {
            return parent;
        }
        return parent.toBuilder()
                .succeededSubQueryCount(state.succeeded)
                .failedSubQueryCount(state.failed)
                .finishedSubQueryRowCount(state.rowCount)
                .build();
    }

    @Override
    public TrackedQuery getStatus(String queryId, String subQueryId) throws QueryTrackerException {
        if (NON_NESTED_QUERY_PLACEHOLDER.equals(subQueryId)) {
            return getStatus(queryId);
        }
        if (subQueryId.startsWith(DynamoDBQueryTrackerEntry.COUNTER_SHARD_PREFIX)) {
            // Counter shard items are internal to the tracker and are not tracked queries.
            return null;
        }
        QueryResponse response = dynamoClient.query(request -> request
                .tableName(trackerTableName)
                .keyConditions(Map.of(
                        QUERY_ID, Condition.builder()
                                .attributeValueList(AttributeValue.fromS(queryId))
                                .comparisonOperator(ComparisonOperator.EQ)
                                .build(),
                        SUB_QUERY_ID, Condition.builder()
                                .attributeValueList(AttributeValue.fromS(subQueryId))
                                .comparisonOperator(ComparisonOperator.EQ)
                                .build())));

        if (response.count() == 0) {
            return null;
        } else if (response.count() > 1) {
            LOGGER.error("Multiple tracked queries returned: {}", response.items());
            throw new QueryTrackerException("More than one query with id " + queryId + " and subquery id "
                    + subQueryId + " was found.");
        }

        return DynamoDBQueryTrackerEntry.toTrackedQuery(response.items().get(0));
    }

    @Override
    public List<TrackedQuery> getAllQueries() {
        ScanIterable response = dynamoClient.scanPaginator(request -> request.tableName(trackerTableName));
        List<Map<String, AttributeValue>> items = response.items().stream().toList();
        Map<String, ParentQueryState> countersByQueryId = new HashMap<>();
        for (Map<String, AttributeValue> item : items) {
            if (isCounterShardItem(item)) {
                countersByQueryId.computeIfAbsent(item.get(QUERY_ID).s(), id -> new ParentQueryState())
                        .addCountersFrom(item);
            }
        }
        return items.stream()
                .filter(item -> !isCounterShardItem(item))
                .map(item -> toTrackedQueryWithCounters(item, countersByQueryId))
                .toList();
    }

    /**
     * Converts an item from the tracker to the model, summing the counters of finished subqueries into
     * a parent query's entry when the scan found counter shards for it.
     *
     * @param  item              the tracker item
     * @param  countersByQueryId sums of the counter shard items found by the scan, by query ID
     * @return                   the tracked query
     */
    private static TrackedQuery toTrackedQueryWithCounters(
            Map<String, AttributeValue> item, Map<String, ParentQueryState> countersByQueryId) {
        TrackedQuery query = DynamoDBQueryTrackerEntry.toTrackedQuery(item);
        if (!NON_NESTED_QUERY_PLACEHOLDER.equals(item.get(SUB_QUERY_ID).s())) {
            return query;
        }
        ParentQueryState counters = countersByQueryId.get(item.get(QUERY_ID).s());
        if (counters == null) {
            return query;
        }
        return query.toBuilder()
                .succeededSubQueryCount(counters.succeeded)
                .failedSubQueryCount(counters.failed)
                .finishedSubQueryRowCount(counters.rowCount)
                .build();
    }

    private static boolean isCounterShardItem(Map<String, AttributeValue> item) {
        return item.get(SUB_QUERY_ID).s().startsWith(DynamoDBQueryTrackerEntry.COUNTER_SHARD_PREFIX);
    }

    @Override
    public List<TrackedQuery> getQueriesWithState(QueryState state) {
        ScanIterable response = dynamoClient.scanPaginator(request -> request
                .tableName(trackerTableName)
                .filterExpression("#LastState = :state")
                .expressionAttributeNames(Map.of("#LastState", LAST_KNOWN_STATE))
                .expressionAttributeValues(Map.of(":state", AttributeValue.fromS(state.toString()))));
        return response.items().stream()
                .map(this::toTrackedQueryJoiningCounters)
                .toList();
    }

    @Override
    public List<TrackedQuery> getFailedQueries() {
        ScanIterable response = dynamoClient.scanPaginator(request -> request
                .tableName(trackerTableName)
                .filterExpression("#LastState = :failed or #LastState = :partiallyFailed")
                .expressionAttributeNames(Map.of("#LastState", LAST_KNOWN_STATE))
                .expressionAttributeValues(Map.of(
                        ":failed", AttributeValue.fromS(QueryState.FAILED.toString()),
                        ":partiallyFailed", AttributeValue.fromS(QueryState.PARTIALLY_FAILED.toString()))));
        return response.items().stream()
                .map(this::toTrackedQueryJoiningCounters)
                .toList();
    }

    /**
     * Converts a tracker item to the model, joining in the summed counters of finished subqueries when the item is
     * a parent query. This is for the state-filtered listings getQueriesWithState and getFailedQueries: their scans
     * exclude the counter shard items, which hold no state attribute, so the shards are read separately with one
     * query per parent query in the result.
     *
     * @param  item the tracker item
     * @return      the tracked query
     */
    private TrackedQuery toTrackedQueryJoiningCounters(Map<String, AttributeValue> item) {
        TrackedQuery query = DynamoDBQueryTrackerEntry.toTrackedQuery(item);
        if (!NON_NESTED_QUERY_PLACEHOLDER.equals(item.get(SUB_QUERY_ID).s())) {
            return query;
        }
        ParentQueryState state = readParentQueryState(query.getQueryId(), false);
        if (!state.countersFound) {
            return query;
        }
        return query.toBuilder()
                .succeededSubQueryCount(state.succeeded)
                .failedSubQueryCount(state.failed)
                .finishedSubQueryRowCount(state.rowCount)
                .build();
    }

    @Override
    public void queryQueued(Query query) {
        updateState(DynamoDBQueryTrackerEntry.withQuery(query).state(QueryState.QUEUED).build());
    }

    @Override
    public void queryInProgress(Query query) {
        updateState(DynamoDBQueryTrackerEntry.withQuery(query).state(QueryState.IN_PROGRESS).build());
    }

    @Override
    public void queryInProgress(LeafPartitionQuery leafQuery) {
        updateState(DynamoDBQueryTrackerEntry.withLeafQuery(leafQuery).state(QueryState.IN_PROGRESS).build());
    }

    @Override
    public void subQueriesCreated(Query query, List<LeafPartitionQuery> subQueries) {
        String attemptId = subQueries.stream()
                .map(LeafPartitionQuery::getAttemptId)
                .filter(Objects::nonNull)
                .findFirst().orElse(null);
        initialiseSubQueryCountersOnParent(query, subQueries.size(), attemptId);
    }

    /**
     * Records on the parent query's item how many sub-queries were created, and resets the counters of finished
     * sub-queries. When a sub-query finishes it increments these counters (this operation is independent of the
     * number of subqueries). This method should be called before any sub-query is submitted for execution, so
     * that the expected count is always recorded by the time a sub-query finishes.
     * <p>
     * Subqueries are not written to the tracker here. Writing an entry per subquery could cause too much load
     * on DynamoDB (as they share a hash key, all updates go one or a small number of partitions).
     * <p>
     * The counters are sharded over several items alongside the parent query's item, because the transactions that
     * count finished subqueries conflict with each other when they touch the same item, and a single counter item
     * cancels most transactions when many subqueries of the same query finish at once. All shards are reset in one
     * transaction, together with the expected count on the parent query's item.
     * <p>
     * The ID of this attempt at processing the query is recorded with the counters. Only subqueries finishing in
     * the same attempt are counted, so that when the whole query is reprocessed, e.g. from a duplicated message for the
     * parent query, subqueries finishing under an older attempt cannot be counted against the reset counters.
     *
     * @param query         the parent query
     * @param subQueryCount the number of sub-queries that were created
     * @param attemptId     an identifier for this attempt at processing the query, null if not set
     */
    private void initialiseSubQueryCountersOnParent(Query query, int subQueryCount, String attemptId) {
        List<TransactWriteItem> items = new ArrayList<>();
        items.add(TransactWriteItem.builder().update(buildExpectedCountUpdate(query.getQueryId(), subQueryCount, attemptId)).build());
        for (int shard = 0; shard < COUNTER_SHARDS; shard++) {
            items.add(TransactWriteItem.builder().update(buildCounterShardReset(query.getQueryId(), shard, attemptId)).build());
        }
        transactWithRetries(items, "registering " + subQueryCount + " subqueries of query " + query.getQueryId());
    }

    private Update buildExpectedCountUpdate(String queryId, int subQueryCount, String attemptId) {
        Map<String, String> attributeNames = new HashMap<>(Map.of(
                "#Expected", DynamoDBQueryTrackerEntry.EXPECTED_SUB_QUERY_COUNT));
        Map<String, AttributeValue> attributeValues = new HashMap<>(Map.of(
                ":expected", AttributeValue.fromN(String.valueOf(subQueryCount))));
        String updateExpression = "SET #Expected = :expected";
        if (attemptId != null) {
            attributeNames.put("#AttemptId", DynamoDBQueryTrackerEntry.ATTEMPT_ID);
            attributeValues.put(":attemptId", AttributeValue.fromS(attemptId));
            updateExpression += ", #AttemptId = :attemptId";
        }
        String setExpectedExpression = updateExpression;
        return Update.builder()
                .tableName(trackerTableName)
                .key(DynamoDBQueryTrackerEntry.getParentKey(queryId))
                .updateExpression(setExpectedExpression)
                .expressionAttributeNames(attributeNames)
                .expressionAttributeValues(attributeValues)
                .build();
    }

    private Update buildCounterShardReset(String queryId, int shard, String attemptId) {
        Map<String, String> attributeNames = new HashMap<>(Map.of(
                "#Succeeded", DynamoDBQueryTrackerEntry.SUCCEEDED_SUB_QUERY_COUNT,
                "#Failed", DynamoDBQueryTrackerEntry.FAILED_SUB_QUERY_COUNT,
                "#Rows", DynamoDBQueryTrackerEntry.FINISHED_SUB_QUERY_ROW_COUNT,
                "#Expiry", DynamoDBQueryTrackerEntry.EXPIRY_DATE));
        Map<String, AttributeValue> attributeValues = new HashMap<>(Map.of(
                ":zero", AttributeValue.fromN("0"),
                ":expiry", expiryDateValue()));
        String updateExpression = "SET #Succeeded = :zero, #Failed = :zero, #Rows = :zero, #Expiry = :expiry";
        if (attemptId != null) {
            attributeNames.put("#AttemptId", DynamoDBQueryTrackerEntry.ATTEMPT_ID);
            attributeValues.put(":attemptId", AttributeValue.fromS(attemptId));
            updateExpression += ", #AttemptId = :attemptId";
        }
        String resetExpression = updateExpression;
        return Update.builder()
                .tableName(trackerTableName)
                .key(DynamoDBQueryTrackerEntry.getCounterShardKey(queryId, shard))
                .updateExpression(resetExpression)
                .expressionAttributeNames(attributeNames)
                .expressionAttributeValues(attributeValues)
                .build();
    }

    private AttributeValue expiryDateValue() {
        return AttributeValue.fromN(String.valueOf(Instant.now().getEpochSecond() + 3600 * 24 * queryTrackerTTL));
    }

    @Override
    public void queryCompleted(Query query, ResultsOutputInfo outputInfo) {
        updateState(DynamoDBQueryTrackerEntry.withQuery(query)
                .completed(outputInfo)
                .build());
    }

    @Override
    public void queryCompleted(LeafPartitionQuery leafQuery, ResultsOutputInfo outputInfo) {
        updateState(DynamoDBQueryTrackerEntry.withLeafQuery(leafQuery)
                .completed(outputInfo)
                .build());
    }

    @Override
    public void queryFailed(Query query, Exception e) {
        updateState(DynamoDBQueryTrackerEntry.withQuery(query)
                .failed(e)
                .build());
    }

    @Override
    public void queryFailed(String queryId, Exception e) {
        updateState(DynamoDBQueryTrackerEntry.builder()
                .queryId(queryId)
                .failed(e)
                .build());
    }

    @Override
    public void queryFailed(LeafPartitionQuery leafQuery, Exception e) {
        updateState(DynamoDBQueryTrackerEntry.withLeafQuery(leafQuery)
                .failed(e)
                .build());
    }

    private void updateState(DynamoDBQueryTrackerEntry entry) {
        if (entry.isSubQuery()) {
            updateSubQueryState(entry);
        } else {
            dynamoClient.updateItem(request -> request
                    .tableName(trackerTableName)
                    .key(entry.getKey())
                    .attributeUpdates(entry.getValueUpdate(queryTrackerTTL)));
        }
    }

    /**
     * Updates the state of a sub-query, and updates the parent query if this update finished the sub-query. The
     * update is rejected if the sub-query was already finished. This means that a duplicate of a message on the
     * sub-query queue cannot revert the state of a finished sub-query, nor count the same sub-query as
     * finished twice against the parent query.
     *
     * @param entry the update to the sub-query
     */
    private void updateSubQueryState(DynamoDBQueryTrackerEntry entry) {
        if (entry.isFinished()) {
            updateFinishedSubQueryState(entry);
        } else {
            try {
                updateSubQueryItem(entry);
            } catch (ConditionalCheckFailedException e) {
                LOGGER.warn("Subquery {} of query {} was already finished in the same attempt, ignoring update to state {}",
                        entry.getSubQueryId(), entry.getQueryId(), entry.getState());
            }
        }
    }

    private void updateSubQueryItem(DynamoDBQueryTrackerEntry entry) {
        Update update = buildSubQueryItemUpdate(entry);
        dynamoClient.updateItem(request -> request
                .tableName(update.tableName())
                .key(update.key())
                .updateExpression(update.updateExpression())
                .conditionExpression(update.conditionExpression())
                .expressionAttributeNames(update.expressionAttributeNames())
                .expressionAttributeValues(update.expressionAttributeValues()));
    }

    /**
     * Applies the terminal (i.e. completed, failed or partially failed) update to a subquery's item and updates
     * the counter shard in a single transaction. This is done in a transaction to ensure that a subquery cannot
     * finish without being counted. If this fails then the subquery will not be marked as finished and either a
     * retry of the update or a redelivery of the subquery's message can succeed resulting in the correct updates.
     * <p>
     * The transaction will be cancelled if the subquery already finished in the same attempt (a duplicate message,
     * which must not be counted twice), or if the parent query's current attempt is different to the one the subquery
     * finished under (the whole query was reprocessed and the counters were reset, so only the new attempt's
     * subqueries may be counted). In either case the parent query is finished if its counters show that all
     * subqueries have already finished.
     *
     * @param entry the terminal update to the subquery
     */
    private void updateFinishedSubQueryState(DynamoDBQueryTrackerEntry entry) {
        Instant startTime = Instant.now();
        try {
            trackFinishedSubQuery(entry);
        } finally {
            LOGGER.info("Spent {} milliseconds updating the query tracker for finished subquery {} of query {}",
                    Duration.between(startTime, Instant.now()).toMillis(), entry.getSubQueryId(), entry.getQueryId());
        }
    }

    private void trackFinishedSubQuery(DynamoDBQueryTrackerEntry entry) {
        try {
            transactWithRetries(List.of(
                    TransactWriteItem.builder().update(buildSubQueryItemUpdate(entry)).build(),
                    TransactWriteItem.builder().update(buildCounterShardUpdate(entry)).build()),
                    "counting finished subquery " + entry.getSubQueryId() + " of query " + entry.getQueryId());
        } catch (TransactionCanceledException e) {
            if (hasConditionFailure(e)) {
                logFinishedSubQueryNotCounted(entry, e);
                finishParentIfCountersComplete(entry);
                return;
            }
            throw e;
        }
        updateStateOfParent(entry);
    }

    /**
     * Performs a transaction, retrying when it is cancelled for a transient reason. Transactions touching the same
     * item conflict with each other instead of queueing, and the SDK does not retry cancellations, so conflicts are
     * expected when many subqueries of the same query finish at once, even with the counters sharded over several
     * items. A cancellation caused by a condition failure is rethrown immediately for the caller to interpret.
     *
     * @param items       the writes to perform in one transaction
     * @param description a description of the operation for logging
     */
    private void transactWithRetries(List<TransactWriteItem> items, String description) {
        try {
            // Every iteration returns on success or throws via the catch block, which rethrows on a condition
            // failure, a non-retryable cancellation, or once MAX_ATTEMPTS_PER_TRANSACTION attempts have failed.
            for (int attempt = 1;; attempt++) {
                transactionConflictBackoff.waitBeforeAttempt(attempt);
                try {
                    dynamoClient.transactWriteItems(request -> request.transactItems(items));
                    return;
                } catch (TransactionCanceledException e) {
                    if (hasConditionFailure(e) || !isRetryableCancellation(e) || attempt >= MAX_ATTEMPTS_PER_TRANSACTION) {
                        throw e;
                    }
                    LOGGER.warn("Transaction cancelled {} on attempt {}, will retry. Cancellation reasons: {}",
                            description, attempt,
                            e.cancellationReasons().stream().map(CancellationReason::code).collect(Collectors.toList()));
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted retrying cancelled transaction " + description, e);
        }
    }

    private static boolean hasConditionFailure(TransactionCanceledException e) {
        return e.cancellationReasons().stream()
                .anyMatch(reason -> "ConditionalCheckFailed".equals(reason.code()));
    }

    private static boolean isRetryableCancellation(TransactionCanceledException e) {
        return e.cancellationReasons().stream()
                .map(CancellationReason::code)
                .filter(code -> !"None".equals(code))
                .allMatch(RETRYABLE_CANCELLATION_CODES::contains);
    }

    private void logFinishedSubQueryNotCounted(DynamoDBQueryTrackerEntry entry, TransactionCanceledException e) {
        if (isConditionFailed(e, 0)) {
            LOGGER.warn("Subquery {} of query {} was already finished in the same attempt, ignoring update to state {}",
                    entry.getSubQueryId(), entry.getQueryId(), entry.getState());
        } else if (isConditionFailed(e, 1)) {
            LOGGER.warn("Subquery {} of query {} finished under attempt {}, which is no longer the current " +
                    "attempt at the query, not counting it against the parent query",
                    entry.getSubQueryId(), entry.getQueryId(), entry.getAttemptId());
        } else {
            throw e;
        }
    }

    private static boolean isConditionFailed(TransactionCanceledException e, int index) {
        List<CancellationReason> reasons = e.cancellationReasons();
        return reasons.size() > index && "ConditionalCheckFailed".equals(reasons.get(index).code());
    }

    /**
     * Builds the update to a subquery's item, creating the item if it does not exist yet. The update is conditional
     * on the subquery not already being finished in the same attempt at the parent query. Within one attempt there
     * is exactly one message per subquery, and only processing that message writes the subquery's terminal state,
     * so a condition failure means the same message was processed again: a duplicate SQS delivery, a retry of a
     * transaction that actually committed, or a re-reported completion. The first write already counted the
     * subquery, so the rejected update must neither revert the state of the finished subquery, nor be counted
     * against the parent query a second time. An update for a different attempt is applied, as the whole query was
     * reprocessed and the counters were reset, e.g. after a duplicated message for the parent query.
     *
     * @param  entry the update to the subquery
     * @return       the update to perform
     */
    private Update buildSubQueryItemUpdate(DynamoDBQueryTrackerEntry entry) {
        Map<String, String> attributeNames = new HashMap<>();
        Map<String, AttributeValue> attributeValues = new HashMap<>();
        List<String> assignments = new ArrayList<>();
        int index = 0;
        for (Map.Entry<String, AttributeValueUpdate> update : entry.getValueUpdate(queryTrackerTTL).entrySet()) {
            String name = "#Attr" + index;
            String value = ":value" + index;
            attributeNames.put(name, update.getKey());
            attributeValues.put(value, update.getValue().value());
            assignments.add(name + " = " + value);
            index++;
        }
        attributeNames.put("#State", LAST_KNOWN_STATE);
        attributeValues.put(":completed", AttributeValue.fromS(QueryState.COMPLETED.name()));
        attributeValues.put(":failed", AttributeValue.fromS(QueryState.FAILED.name()));
        attributeValues.put(":partiallyFailed", AttributeValue.fromS(QueryState.PARTIALLY_FAILED.name()));
        String notFinishedCondition = "attribute_not_exists(#State) OR NOT #State IN (:completed, :failed, :partiallyFailed)";
        String conditionExpression;
        if (entry.getAttemptId() != null) {
            attributeNames.put("#AttemptId", DynamoDBQueryTrackerEntry.ATTEMPT_ID);
            attributeValues.put(":attemptId", AttributeValue.fromS(entry.getAttemptId()));
            conditionExpression = notFinishedCondition + " OR #AttemptId <> :attemptId";
        } else {
            conditionExpression = notFinishedCondition;
        }
        return Update.builder()
                .tableName(trackerTableName)
                .key(entry.getKey())
                .updateExpression("SET " + String.join(", ", assignments))
                .conditionExpression(conditionExpression)
                .expressionAttributeNames(attributeNames)
                .expressionAttributeValues(attributeValues)
                .build();
    }

    /**
     * Builds the increment counting a finished subquery against one shard of the parent query's counters. The shard
     * is chosen by hashing the subquery ID, so duplicate messages for the same subquery always address the same
     * shard. When the subquery holds an attempt ID, the increment is conditional on the shard's current attempt
     * being the same one, so that a subquery finishing under a previous attempt at the query cannot be counted
     * after the counters were reset for a new attempt.
     *
     * @param  leafQueryEntry the terminal update to the subquery
     * @return                the update to perform
     */
    private Update buildCounterShardUpdate(DynamoDBQueryTrackerEntry leafQueryEntry) {
        boolean succeeded = leafQueryEntry.getState() == QueryState.COMPLETED;
        int shard = Math.floorMod(leafQueryEntry.getSubQueryId().hashCode(), COUNTER_SHARDS);
        Map<String, String> attributeNames = new HashMap<>(Map.of(
                "#Succeeded", DynamoDBQueryTrackerEntry.SUCCEEDED_SUB_QUERY_COUNT,
                "#Failed", DynamoDBQueryTrackerEntry.FAILED_SUB_QUERY_COUNT,
                "#Rows", DynamoDBQueryTrackerEntry.FINISHED_SUB_QUERY_ROW_COUNT,
                "#Expiry", DynamoDBQueryTrackerEntry.EXPIRY_DATE));
        Map<String, AttributeValue> attributeValues = new HashMap<>(Map.of(
                ":succeeded", AttributeValue.fromN(succeeded ? "1" : "0"),
                ":failed", AttributeValue.fromN(succeeded ? "0" : "1"),
                ":rows", AttributeValue.fromN(String.valueOf(leafQueryEntry.getRowCount())),
                ":expiry", expiryDateValue()));
        String conditionExpression;
        if (leafQueryEntry.getAttemptId() != null) {
            attributeNames.put("#AttemptId", DynamoDBQueryTrackerEntry.ATTEMPT_ID);
            attributeValues.put(":attemptId", AttributeValue.fromS(leafQueryEntry.getAttemptId()));
            conditionExpression = "attribute_not_exists(#AttemptId) OR #AttemptId = :attemptId";
        } else {
            conditionExpression = null;
        }
        return Update.builder()
                .tableName(trackerTableName)
                .key(DynamoDBQueryTrackerEntry.getCounterShardKey(leafQueryEntry.getQueryId(), shard))
                .updateExpression("ADD #Succeeded :succeeded, #Failed :failed, #Rows :rows SET #Expiry = :expiry")
                .conditionExpression(conditionExpression)
                .expressionAttributeNames(attributeNames)
                .expressionAttributeValues(attributeValues)
                .build();
    }

    private void updateStateOfParent(DynamoDBQueryTrackerEntry leafQueryEntry) {
        // Use a consistent read here because this is used to make a decision on whether the query has completed or
        // not and we want the query to see the count increment which was just made.
        ParentQueryState parentState = readParentQueryState(leafQueryEntry.getQueryId(), true);
        if (parentState.expected != null) {
            updateStateOfParentFromCounters(leafQueryEntry, parentState);
        } else {
            // Fallback for subqueries that were never registered with subQueriesCreated, so no expected count
            // exists. The deployed query pipeline always registers subqueries before submitting them, so this only
            // supports direct use of the tracker, including by its tests. It reads every item held for the query,
            // so it is only viable for queries with few subqueries.
            updateStateOfParentFromSubQueryItems(leafQueryEntry);
        }
    }

    /**
     * Finishes the parent query if its counters show that all subqueries have finished but the write that finishes
     * the parent query was lost. This is called when a terminal update to a subquery is rejected, e.g. for a
     * duplicate message, so that a duplicate can repair a query that would otherwise be stuck in progress.
     *
     * @param leafQueryEntry the terminal update to the subquery
     */
    private void finishParentIfCountersComplete(DynamoDBQueryTrackerEntry leafQueryEntry) {
        // Use a consistent read here because this is used to make a decision on whether the query has completed or
        // not and we want the query to see every increment already committed by other completions; this runs after a
        // rejected update, so this caller made no increment of its own.
        ParentQueryState parentState = readParentQueryState(leafQueryEntry.getQueryId(), true);
        if (parentState.expected != null) {
            updateStateOfParentFromCounters(leafQueryEntry, parentState);
        }
    }

    /**
     * Reads the parent query's item and the shards of its counters of finished subqueries, in one query on the
     * query's partition. The parent's sort key placeholder is a prefix of every counter shard's sort key, so a
     * single key condition covers them all.
     *
     * @param  queryId        the query ID
     * @param  consistentRead whether the query must see all previously completed writes
     * @return                the parent query's item, if present, with the summed counters
     */
    private ParentQueryState readParentQueryState(String queryId, boolean consistentRead) {
        ParentQueryState state = new ParentQueryState();
        QueryIterable pages = dynamoClient.queryPaginator(request -> request
                .tableName(trackerTableName)
                .consistentRead(consistentRead)
                .keyConditionExpression("#QueryId = :queryId AND begins_with(#SubQueryId, :placeholder)")
                .expressionAttributeNames(Map.of("#QueryId", QUERY_ID, "#SubQueryId", SUB_QUERY_ID))
                .expressionAttributeValues(Map.of(
                        ":queryId", AttributeValue.fromS(queryId),
                        ":placeholder", AttributeValue.fromS(NON_NESTED_QUERY_PLACEHOLDER))));
        for (Map<String, AttributeValue> item : pages.items()) {
            String sortKey = item.get(SUB_QUERY_ID).s();
            if (NON_NESTED_QUERY_PLACEHOLDER.equals(sortKey)) {
                state.parentItem = item;
                state.expected = readOptionalLongAttribute(item, DynamoDBQueryTrackerEntry.EXPECTED_SUB_QUERY_COUNT);
            } else if (sortKey.startsWith(DynamoDBQueryTrackerEntry.COUNTER_SHARD_PREFIX)) {
                state.addCountersFrom(item);
            }
        }
        return state;
    }

    /**
     * The state of a parent query: its tracker item and the summed counters of its finished subqueries.
     */
    private static class ParentQueryState {
        private Map<String, AttributeValue> parentItem;
        private Long expected;
        private boolean countersFound;
        private long succeeded;
        private long failed;
        private long rowCount;

        void addCountersFrom(Map<String, AttributeValue> item) {
            if (!item.containsKey(DynamoDBQueryTrackerEntry.SUCCEEDED_SUB_QUERY_COUNT)
                    && !item.containsKey(DynamoDBQueryTrackerEntry.FAILED_SUB_QUERY_COUNT)) {
                return;
            }
            countersFound = true;
            succeeded += readLongAttributeOrZero(item, DynamoDBQueryTrackerEntry.SUCCEEDED_SUB_QUERY_COUNT);
            failed += readLongAttributeOrZero(item, DynamoDBQueryTrackerEntry.FAILED_SUB_QUERY_COUNT);
            rowCount += readLongAttributeOrZero(item, DynamoDBQueryTrackerEntry.FINISHED_SUB_QUERY_ROW_COUNT);
        }
    }

    /**
     * Finishes the parent query if the summed counters show that all subqueries have finished. The counters are
     * read after the subquery's completion was counted, so the completion that takes the counters to the expected
     * count always sees them complete. Rarely more than one completion may see complete counters and finish the
     * parent, which is harmless as they write the same state and totals.
     *
     * @param leafQueryEntry the update that finished the sub-query
     * @param parentState    the parent query's item and summed counters
     */
    private void updateStateOfParentFromCounters(DynamoDBQueryTrackerEntry leafQueryEntry, ParentQueryState parentState) {
        long expected = parentState.expected;
        long finished = parentState.succeeded + parentState.failed;
        if (finished < expected) {
            LOGGER.info("Found {} of {} sub-queries have finished for query {}",
                    finished, expected, leafQueryEntry.getQueryId());
            return;
        }
        QueryState newState;
        if (parentState.failed == 0) {
            newState = QueryState.COMPLETED;
        } else if (parentState.succeeded == 0) {
            newState = QueryState.FAILED;
        } else {
            newState = QueryState.PARTIALLY_FAILED;
        }
        LOGGER.info("Updating state of parent to {}", newState);
        updateState(leafQueryEntry.updateParent(newState, parentState.rowCount));
    }

    private void updateStateOfParentFromSubQueryItems(DynamoDBQueryTrackerEntry leafQueryEntry) {
        LOGGER.warn("No expected subquery count was recorded for query {}, so its subqueries were not registered " +
                "before they ran. Falling back to reading every entry for the query to check whether it has " +
                "finished. This is not expected when queries run through the query processor lambda.",
                leafQueryEntry.getQueryId());
        QueryIterable trackedQueries = dynamoClient.queryPaginator(request -> request
                .tableName(trackerTableName)
                .consistentRead(true)
                .keyConditions(Map.of(
                        QUERY_ID, Condition.builder()
                                .attributeValueList(AttributeValue.fromS(leafQueryEntry.getQueryId()))
                                .comparisonOperator(ComparisonOperator.EQ)
                                .build())));

        List<TrackedQuery> children = trackedQueries.items().stream()
                .filter(item -> !NON_NESTED_QUERY_PLACEHOLDER.equals(item.get(SUB_QUERY_ID).s()))
                .filter(item -> !isCounterShardItem(item))
                .map(DynamoDBQueryTrackerEntry::toTrackedQuery)
                .collect(Collectors.toList());

        Optional<QueryState> parentState = QueryState.getParentStateIfFinished(leafQueryEntry.getQueryId(), children);

        if (parentState.isPresent()) {
            long totalRowCount = children.stream()
                    .mapToLong(query -> query.getRowCount() != null ? query.getRowCount() : 0).sum();
            LOGGER.info("Updating state of parent to {}", parentState.get());
            updateState(leafQueryEntry.updateParent(parentState.get(), totalRowCount));
        }
    }

    private static long readLongAttributeOrZero(Map<String, AttributeValue> item, String attribute) {
        AttributeValue value = item.get(attribute);
        return value != null ? Long.parseLong(value.n()) : 0;
    }

    private static Long readOptionalLongAttribute(Map<String, AttributeValue> item, String attribute) {
        AttributeValue value = item.get(attribute);
        return value != null ? Long.valueOf(value.n()) : null;
    }

}
