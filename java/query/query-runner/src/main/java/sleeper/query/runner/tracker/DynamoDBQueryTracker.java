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
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.services.dynamodb.paginators.QueryIterable;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ExponentialBackoffWithJitter.WaitRange;
import sleeper.core.util.SplitIntoBatches;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.tracker.QueryState;
import sleeper.query.core.tracker.QueryStatusReportListener;
import sleeper.query.core.tracker.QueryTrackerException;
import sleeper.query.core.tracker.QueryTrackerStore;
import sleeper.query.core.tracker.TrackedQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    public static final WaitRange UNPROCESSED_WRITES_WAIT_RANGE = WaitRange.firstAndMaxWaitCeilingSecs(1, 30);
    private static final int DYNAMO_MAX_BATCH_WRITE_SIZE = 25;
    private static final int MAX_ATTEMPTS_PER_BATCH_WRITE = 10;
    public static final String NON_NESTED_QUERY_PLACEHOLDER = DynamoDBQueryTrackerEntry.NON_NESTED_QUERY_PLACEHOLDER;
    public static final String QUERY_ID = DynamoDBQueryTrackerEntry.QUERY_ID;
    public static final String SUB_QUERY_ID = DynamoDBQueryTrackerEntry.SUB_QUERY_ID;
    public static final String LAST_KNOWN_STATE = DynamoDBQueryTrackerEntry.LAST_KNOWN_STATE;

    private final DynamoDbClient dynamoClient;
    private final String trackerTableName;
    private final long queryTrackerTTL;
    private final ExponentialBackoffWithJitter unprocessedWritesBackoff;

    public DynamoDBQueryTracker(InstanceProperties instanceProperties, DynamoDbClient dynamoClient) {
        this(instanceProperties, dynamoClient, new ExponentialBackoffWithJitter(UNPROCESSED_WRITES_WAIT_RANGE));
    }

    public DynamoDBQueryTracker(InstanceProperties instanceProperties, DynamoDbClient dynamoClient, ExponentialBackoffWithJitter unprocessedWritesBackoff) {
        this.trackerTableName = instanceProperties.get(QUERY_TRACKER_TABLE_NAME);
        this.queryTrackerTTL = instanceProperties.getLong(QUERY_TRACKER_ITEM_TTL_IN_DAYS);
        this.dynamoClient = dynamoClient;
        this.unprocessedWritesBackoff = unprocessedWritesBackoff;
    }

    public DynamoDBQueryTracker(Map<String, String> destinationConfig) {
        this.trackerTableName = destinationConfig.get(QUERY_TRACKER_TABLE_NAME.getPropertyName());
        String ttl = destinationConfig.get(QUERY_TRACKER_ITEM_TTL_IN_DAYS.getPropertyName());
        this.queryTrackerTTL = Long.parseLong(ttl != null ? ttl : QUERY_TRACKER_ITEM_TTL_IN_DAYS.getDefaultValue());
        this.dynamoClient = DynamoDbClient.create();
        this.unprocessedWritesBackoff = new ExponentialBackoffWithJitter(UNPROCESSED_WRITES_WAIT_RANGE);
    }

    @Override
    public TrackedQuery getStatus(String queryId) throws QueryTrackerException {
        return getStatus(queryId, NON_NESTED_QUERY_PLACEHOLDER);
    }

    @Override
    public TrackedQuery getStatus(String queryId, String subQueryId) throws QueryTrackerException {
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
        return response.items().stream()
                .map(DynamoDBQueryTrackerEntry::toTrackedQuery)
                .toList();
    }

    @Override
    public List<TrackedQuery> getQueriesWithState(QueryState state) {
        ScanIterable response = dynamoClient.scanPaginator(request -> request
                .tableName(trackerTableName)
                .filterExpression("#LastState = :state")
                .expressionAttributeNames(Map.of("#LastState", LAST_KNOWN_STATE))
                .expressionAttributeValues(Map.of(":state", AttributeValue.fromS(state.toString()))));
        return response.items().stream()
                .map(DynamoDBQueryTrackerEntry::toTrackedQuery)
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
                .map(DynamoDBQueryTrackerEntry::toTrackedQuery)
                .toList();
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
        SplitIntoBatches.splitListIntoBatchesOf(DYNAMO_MAX_BATCH_WRITE_SIZE, subQueries)
                .forEach(this::putQueuedSubQueries);
        initialiseSubQueryCountersOnParent(query, subQueries.size());
    }

    /**
     * Records on the parent query's item how many sub-queries were created, and resets the counters of finished
     * sub-queries. When a sub-query finishes it increments these counters (this operation is independent of the
     * number of subqueries). This method should be called before any sub-query is submitted for execution, so
     * that the expected count is always recorded by the time a sub-query finishes.
     *
     * @param query         the parent query
     * @param subQueryCount the number of sub-queries that were created
     */
    private void initialiseSubQueryCountersOnParent(Query query, int subQueryCount) {
        dynamoClient.updateItem(request -> request
                .tableName(trackerTableName)
                .key(DynamoDBQueryTrackerEntry.getParentKey(query.getQueryId()))
                .updateExpression("SET #Expected = :expected, #Succeeded = :zero, #Failed = :zero, #Rows = :zero")
                .expressionAttributeNames(Map.of(
                        "#Expected", DynamoDBQueryTrackerEntry.EXPECTED_SUB_QUERY_COUNT,
                        "#Succeeded", DynamoDBQueryTrackerEntry.SUCCEEDED_SUB_QUERY_COUNT,
                        "#Failed", DynamoDBQueryTrackerEntry.FAILED_SUB_QUERY_COUNT,
                        "#Rows", DynamoDBQueryTrackerEntry.TOTAL_SUB_QUERY_ROW_COUNT))
                .expressionAttributeValues(Map.of(
                        ":expected", AttributeValue.fromN(String.valueOf(subQueryCount)),
                        ":zero", AttributeValue.fromN("0"))));
    }

    private void putQueuedSubQueries(List<LeafPartitionQuery> subQueries) {
        Map<String, List<WriteRequest>> writesByTable = Map.of(trackerTableName, subQueries.stream()
                .map(subQuery -> DynamoDBQueryTrackerEntry.withLeafQuery(subQuery).state(QueryState.QUEUED).build())
                .map(entry -> WriteRequest.builder()
                        .putRequest(PutRequest.builder()
                                .item(entry.getItem(queryTrackerTTL))
                                .build())
                        .build())
                .toList());
        try {
            for (int attempt = 1; !writesByTable.isEmpty(); attempt++) {
                if (attempt > MAX_ATTEMPTS_PER_BATCH_WRITE) {
                    throw new RuntimeException("Failed to track creation of sub-queries, too many unprocessed writes " +
                            "after " + MAX_ATTEMPTS_PER_BATCH_WRITE + " attempts");
                }
                unprocessedWritesBackoff.waitBeforeAttempt(attempt);
                Map<String, List<WriteRequest>> requestItems = writesByTable;
                BatchWriteItemResponse response = dynamoClient.batchWriteItem(request -> request.requestItems(requestItems));
                writesByTable = response.unprocessedItems();
                if (!writesByTable.isEmpty()) {
                    LOGGER.warn("Found {} unprocessed writes tracking creation of sub-queries, retrying",
                            writesByTable.values().stream().mapToInt(List::size).sum());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted retrying unprocessed writes tracking creation of sub-queries", e);
        }
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
        try {
            updateSubQueryItemUnlessAlreadyFinished(entry);
        } catch (ConditionalCheckFailedException e) {
            LOGGER.warn("Sub-query {} of query {} was already finished, ignoring update to state {}",
                    entry.getSubQueryId(), entry.getQueryId(), entry.getState());
            return;
        }
        if (entry.isFinished()) {
            updateStateOfParent(entry);
        }
    }

    private void updateSubQueryItemUnlessAlreadyFinished(DynamoDBQueryTrackerEntry entry) {
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
        String updateExpression = "SET " + String.join(", ", assignments);
        dynamoClient.updateItem(request -> request
                .tableName(trackerTableName)
                .key(entry.getKey())
                .updateExpression(updateExpression)
                .conditionExpression("attribute_not_exists(#State) OR NOT #State IN (:completed, :failed, :partiallyFailed)")
                .expressionAttributeNames(attributeNames)
                .expressionAttributeValues(attributeValues));
    }

    private void updateStateOfParent(DynamoDBQueryTrackerEntry leafQueryEntry) {
        Map<String, AttributeValue> parentItem = incrementFinishedSubQueryCountersOnParent(leafQueryEntry);
        if (parentItem.containsKey(DynamoDBQueryTrackerEntry.EXPECTED_SUB_QUERY_COUNT)) {
            updateStateOfParentFromCounters(leafQueryEntry, parentItem);
        } else {
            // This is a temporary method to deal with queries which were generated by previous queries,
            updateStateOfParentFromSubQueryItems(leafQueryEntry);
        }
    }

    /**
     * Atomically increments the counters of finished sub-queries held on the parent query's item, recording whether
     * this sub-query succeeded or failed, and how many rows it returned.
     *
     * @param  leafQueryEntry the update that finished the sub-query
     * @return                the attributes of the parent query's item after the increment
     */
    private Map<String, AttributeValue> incrementFinishedSubQueryCountersOnParent(DynamoDBQueryTrackerEntry leafQueryEntry) {
        boolean succeeded = leafQueryEntry.getState() == QueryState.COMPLETED;
        UpdateItemResponse response = dynamoClient.updateItem(request -> request
                .tableName(trackerTableName)
                .key(leafQueryEntry.getParentKey())
                .updateExpression("ADD #Succeeded :succeeded, #Failed :failed, #Rows :rows")
                .expressionAttributeNames(Map.of(
                        "#Succeeded", DynamoDBQueryTrackerEntry.SUCCEEDED_SUB_QUERY_COUNT,
                        "#Failed", DynamoDBQueryTrackerEntry.FAILED_SUB_QUERY_COUNT,
                        "#Rows", DynamoDBQueryTrackerEntry.TOTAL_SUB_QUERY_ROW_COUNT))
                .expressionAttributeValues(Map.of(
                        ":succeeded", AttributeValue.fromN(succeeded ? "1" : "0"),
                        ":failed", AttributeValue.fromN(succeeded ? "0" : "1"),
                        ":rows", AttributeValue.fromN(String.valueOf(leafQueryEntry.getRowCount()))))
                .returnValues(ReturnValue.ALL_NEW));
        return response.attributes();
    }

    /**
     * Finishes the parent query if the counters on its item show that all sub-queries have finished. The increment
     * of the counters is atomic, so exactly one sub-query observes the counters reaching the expected count, and
     * only that sub-query updates the parent query's state.
     *
     * @param leafQueryEntry the update that finished the sub-query
     * @param parentItem     the attributes of the parent query's item after the increment
     */
    private void updateStateOfParentFromCounters(DynamoDBQueryTrackerEntry leafQueryEntry, Map<String, AttributeValue> parentItem) {
        long expected = readLongAttribute(parentItem, DynamoDBQueryTrackerEntry.EXPECTED_SUB_QUERY_COUNT);
        long succeeded = readLongAttribute(parentItem, DynamoDBQueryTrackerEntry.SUCCEEDED_SUB_QUERY_COUNT);
        long failed = readLongAttribute(parentItem, DynamoDBQueryTrackerEntry.FAILED_SUB_QUERY_COUNT);
        long finished = succeeded + failed;
        if (finished < expected) {
            LOGGER.info("Found {} of {} sub-queries have finished for query {}",
                    finished, expected, leafQueryEntry.getQueryId());
            return;
        }
        QueryState parentState;
        if (failed == 0) {
            parentState = QueryState.COMPLETED;
        } else if (succeeded == 0) {
            parentState = QueryState.FAILED;
        } else {
            parentState = QueryState.PARTIALLY_FAILED;
        }
        long totalRowCount = readLongAttribute(parentItem, DynamoDBQueryTrackerEntry.TOTAL_SUB_QUERY_ROW_COUNT);
        LOGGER.info("Updating state of parent to {}", parentState);
        updateState(leafQueryEntry.updateParent(parentState, totalRowCount));
    }

    private void updateStateOfParentFromSubQueryItems(DynamoDBQueryTrackerEntry leafQueryEntry) {
        QueryIterable trackedQueries = dynamoClient.queryPaginator(request -> request
                .tableName(trackerTableName)
                .consistentRead(true)
                .keyConditions(Map.of(
                        QUERY_ID, Condition.builder()
                                .attributeValueList(AttributeValue.fromS(leafQueryEntry.getQueryId()))
                                .comparisonOperator(ComparisonOperator.EQ)
                                .build())));

        List<TrackedQuery> children = trackedQueries.items().stream()
                .map(DynamoDBQueryTrackerEntry::toTrackedQuery)
                .filter(trackedQuery -> !trackedQuery.getSubQueryId().equals(NON_NESTED_QUERY_PLACEHOLDER))
                .collect(Collectors.toList());

        Optional<QueryState> parentState = QueryState.getParentStateIfFinished(leafQueryEntry.getQueryId(), children);

        if (parentState.isPresent()) {
            long totalRowCount = children.stream()
                    .mapToLong(query -> query.getRowCount() != null ? query.getRowCount() : 0).sum();
            LOGGER.info("Updating state of parent to {}", parentState.get());
            updateState(leafQueryEntry.updateParent(parentState.get(), totalRowCount));
        }
    }

    private static long readLongAttribute(Map<String, AttributeValue> item, String attribute) {
        return Long.parseLong(item.get(attribute).n());
    }

}
