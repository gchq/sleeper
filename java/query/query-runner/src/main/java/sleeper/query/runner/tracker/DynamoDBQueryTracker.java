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
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.tracker.QueryState;
import sleeper.query.core.tracker.QueryStatusReportListener;
import sleeper.query.core.tracker.QueryTrackerException;
import sleeper.query.core.tracker.QueryTrackerStore;
import sleeper.query.core.tracker.TrackedQuery;

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
    public static final String NON_NESTED_QUERY_PLACEHOLDER = DynamoDBQueryTrackerEntry.NON_NESTED_QUERY_PLACEHOLDER;
    public static final String QUERY_ID = DynamoDBQueryTrackerEntry.QUERY_ID;
    public static final String SUB_QUERY_ID = DynamoDBQueryTrackerEntry.SUB_QUERY_ID;
    public static final String LAST_KNOWN_STATE = DynamoDBQueryTrackerEntry.LAST_KNOWN_STATE;

    private final DynamoDbClient dynamoClient;
    private final String trackerTableName;
    private final long queryTrackerTTL;

    public DynamoDBQueryTracker(InstanceProperties instanceProperties, DynamoDbClient dynamoClient) {
        this.trackerTableName = instanceProperties.get(QUERY_TRACKER_TABLE_NAME);
        this.queryTrackerTTL = instanceProperties.getLong(QUERY_TRACKER_ITEM_TTL_IN_DAYS);
        this.dynamoClient = dynamoClient;
    }

    public DynamoDBQueryTracker(Map<String, String> destinationConfig) {
        this.trackerTableName = destinationConfig.get(QUERY_TRACKER_TABLE_NAME.getPropertyName());
        String ttl = destinationConfig.get(QUERY_TRACKER_ITEM_TTL_IN_DAYS.getPropertyName());
        this.queryTrackerTTL = Long.parseLong(ttl != null ? ttl : QUERY_TRACKER_ITEM_TTL_IN_DAYS.getDefaultValue());
        this.dynamoClient = DynamoDbClient.create();
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
        ScanResponse response = dynamoClient.scan(request -> request.tableName(trackerTableName));
        return response.items().stream()
                .map(DynamoDBQueryTrackerEntry::toTrackedQuery)
                .toList();
    }

    @Override
    public List<TrackedQuery> getQueriesWithState(QueryState state) {
        ScanResponse response = dynamoClient.scan(request -> request
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
        ScanResponse response = dynamoClient.scan(request -> request
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
        subQueries.forEach(subQuery -> updateState(
                DynamoDBQueryTrackerEntry.withLeafQuery(subQuery).state(QueryState.QUEUED).build()));
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
        long now = System.currentTimeMillis() / 1000;
        long expiryDate = now + (60 * 60 * 24 * queryTrackerTTL);

        StringBuilder expression = new StringBuilder("SET #LastUpdate = :now,"
                + " #FirstUpdate = if_not_exists(#FirstUpdate, :now),"
                + " #Expiry = :expiry, #RowCount = :rows, #State = :state");
        Map<String, String> names = new HashMap<>(Map.of(
                "#FirstUpdate", DynamoDBQueryTrackerEntry.FIRST_UPDATE_TIME,
                "#LastUpdate", DynamoDBQueryTrackerEntry.LAST_UPDATE_TIME,
                "#Expiry", DynamoDBQueryTrackerEntry.EXPIRY_DATE,
                "#RowCount", DynamoDBQueryTrackerEntry.ROW_COUNT,
                "#State", LAST_KNOWN_STATE));
        Map<String, AttributeValue> values = new HashMap<>(Map.of(
                ":now", AttributeValue.fromN(String.valueOf(now)),
                ":expiry", AttributeValue.fromN(String.valueOf(expiryDate)),
                ":rows", AttributeValue.fromN(String.valueOf(entry.getRowCount())),
                ":state", AttributeValue.fromS(entry.getState().name())));
        if (entry.getTableId() != null) {
            expression.append(", #TableId = :tableId");
            names.put("#TableId", DynamoDBQueryTrackerEntry.TABLE_ID);
            values.put(":tableId", AttributeValue.fromS(entry.getTableId()));
        }
        if (entry.getErrorMessage() != null) {
            expression.append(", #Errors = :errors");
            names.put("#Errors", DynamoDBQueryTrackerEntry.ERROR_MESSAGE);
            values.put(":errors", AttributeValue.fromS(entry.getErrorMessage()));
        }

        dynamoClient.updateItem(request -> request
                .tableName(trackerTableName)
                .key(entry.getKey())
                .updateExpression(expression.toString())
                .expressionAttributeNames(names)
                .expressionAttributeValues(values));
        if (entry.isUpdateParent()) {
            updateStateOfParent(entry);
        }
    }

    private void updateStateOfParent(DynamoDBQueryTrackerEntry leafQueryEntry) {
        List<Map<String, AttributeValue>> trackedQueries = dynamoClient.query(request -> request
                .tableName(trackerTableName)
                .consistentRead(true)
                .keyConditions(Map.of(
                        QUERY_ID, Condition.builder()
                                .attributeValueList(AttributeValue.fromS(leafQueryEntry.getQueryId()))
                                .comparisonOperator(ComparisonOperator.EQ)
                                .build())))
                .items();

        List<TrackedQuery> children = trackedQueries.stream()
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

}
