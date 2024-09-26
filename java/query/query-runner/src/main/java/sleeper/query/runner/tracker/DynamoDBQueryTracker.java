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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.query.model.LeafPartitionQuery;
import sleeper.query.model.Query;
import sleeper.query.output.ResultsOutputInfo;
import sleeper.query.tracker.QueryState;
import sleeper.query.tracker.QueryStatusReportListener;
import sleeper.query.tracker.QueryTrackerException;
import sleeper.query.tracker.QueryTrackerStore;
import sleeper.query.tracker.TrackedQuery;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_TRACKER_TABLE_NAME;
import static sleeper.core.properties.instance.QueryProperty.QUERY_TRACKER_ITEM_TTL_IN_DAYS;
import static sleeper.query.runner.tracker.DynamoDBQueryTrackerEntry.LAST_KNOWN_STATE;

/**
 * The query tracker updates and keeps track of the status of queries so that clients
 * can see how complete it is or if part or all of the query failed.
 */
public class DynamoDBQueryTracker implements QueryStatusReportListener, QueryTrackerStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBQueryTracker.class);

    public static final String DESTINATION = "DYNAMODB";
    public static final String NON_NESTED_QUERY_PLACEHOLDER = "-";
    public static final String QUERY_ID = DynamoDBQueryTrackerEntry.QUERY_ID;
    public static final String SUB_QUERY_ID = DynamoDBQueryTrackerEntry.SUB_QUERY_ID;

    private final AmazonDynamoDB dynamoDB;
    private final String trackerTableName;
    private final long queryTrackerTTL;

    public DynamoDBQueryTracker(InstanceProperties instanceProperties, AmazonDynamoDB dynamoDB) {
        this.trackerTableName = instanceProperties.get(QUERY_TRACKER_TABLE_NAME);
        this.queryTrackerTTL = instanceProperties.getLong(QUERY_TRACKER_ITEM_TTL_IN_DAYS);
        this.dynamoDB = dynamoDB;
    }

    public DynamoDBQueryTracker(Map<String, String> destinationConfig) {
        this.trackerTableName = destinationConfig.get(QUERY_TRACKER_TABLE_NAME.getPropertyName());
        String ttl = destinationConfig.get(QUERY_TRACKER_ITEM_TTL_IN_DAYS.getPropertyName());
        this.queryTrackerTTL = Long.parseLong(ttl != null ? ttl : QUERY_TRACKER_ITEM_TTL_IN_DAYS.getDefaultValue());
        this.dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
    }

    @Override
    public TrackedQuery getStatus(String queryId) throws QueryTrackerException {
        return getStatus(queryId, NON_NESTED_QUERY_PLACEHOLDER);
    }

    @Override
    public TrackedQuery getStatus(String queryId, String subQueryId) throws QueryTrackerException {
        QueryResult result = dynamoDB.query(new QueryRequest()
                .withTableName(trackerTableName)
                .addKeyConditionsEntry(QUERY_ID, new Condition()
                        .withAttributeValueList(new AttributeValue(queryId))
                        .withComparisonOperator(ComparisonOperator.EQ))
                .addKeyConditionsEntry(SUB_QUERY_ID, new Condition()
                        .withAttributeValueList(new AttributeValue(subQueryId))
                        .withComparisonOperator(ComparisonOperator.EQ)));

        if (result.getCount() == 0) {
            return null;
        } else if (result.getCount() > 1) {
            LOGGER.error("Multiple tracked queries returned: {}", result.getItems());
            throw new QueryTrackerException("More than one query with id " + queryId + " and subquery id "
                    + subQueryId + " was found.");
        }

        return DynamoDBQueryTrackerEntry.toTrackedQuery(result.getItems().get(0));
    }

    @Override
    public List<TrackedQuery> getAllQueries() {
        ScanResult result = dynamoDB.scan(new ScanRequest().withTableName(trackerTableName));
        return result.getItems().stream()
                .map(DynamoDBQueryTrackerEntry::toTrackedQuery)
                .collect(Collectors.toList());
    }

    @Override
    public List<TrackedQuery> getQueriesWithState(QueryState queryState) {
        ScanResult result = dynamoDB.scan(new ScanRequest()
                .withTableName(trackerTableName)
                .withExpressionAttributeValues(Map.of(":state", new AttributeValue().withS(queryState.toString())))
                .withFilterExpression(LAST_KNOWN_STATE + " = :state"));
        return result.getItems().stream()
                .map(DynamoDBQueryTrackerEntry::toTrackedQuery)
                .collect(Collectors.toList());
    }

    @Override
    public List<TrackedQuery> getFailedQueries() {
        ScanResult result = dynamoDB.scan(new ScanRequest()
                .withTableName(trackerTableName)
                .withExpressionAttributeValues(Map.of(
                        ":failed", new AttributeValue().withS(QueryState.FAILED.toString()),
                        ":partiallyFailed", new AttributeValue().withS(QueryState.PARTIALLY_FAILED.toString())))
                .withFilterExpression(LAST_KNOWN_STATE + " = :failed or " + LAST_KNOWN_STATE + " = :partiallyFailed"));
        return result.getItems().stream()
                .map(DynamoDBQueryTrackerEntry::toTrackedQuery)
                .collect(Collectors.toList());
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
        dynamoDB.updateItem(new UpdateItemRequest(trackerTableName,
                entry.getKey(), entry.getValueUpdate(queryTrackerTTL)));
        if (entry.isUpdateParent()) {
            updateStateOfParent(entry);
        }
    }

    private void updateStateOfParent(DynamoDBQueryTrackerEntry leafQueryEntry) {
        List<Map<String, AttributeValue>> trackedQueries = dynamoDB.query(new QueryRequest()
                .withTableName(trackerTableName)
                .withConsistentRead(true)
                .addKeyConditionsEntry(QUERY_ID, new Condition()
                        .withAttributeValueList(new AttributeValue(leafQueryEntry.getQueryId()))
                        .withComparisonOperator(ComparisonOperator.EQ)))
                .getItems();

        List<TrackedQuery> children = trackedQueries.stream()
                .map(DynamoDBQueryTrackerEntry::toTrackedQuery)
                .filter(trackedQuery -> !trackedQuery.getSubQueryId().equals(NON_NESTED_QUERY_PLACEHOLDER))
                .collect(Collectors.toList());

        QueryState parentState = getParentState(children);

        if (parentState != null) {
            long totalRecordCount = children.stream()
                    .mapToLong(query -> query.getRecordCount() != null ? query.getRecordCount() : 0).sum();
            LOGGER.info("Updating state of parent to {}", parentState);
            updateState(leafQueryEntry.updateParent(parentState, totalRecordCount));
        }
    }

    private QueryState getParentState(List<TrackedQuery> children) {
        boolean allCompleted = true;
        boolean allSucceeded = true;
        boolean allFailed = true;
        long activeCount = 0;
        for (TrackedQuery child : children) {
            switch (child.getLastKnownState()) {
                case FAILED:
                case PARTIALLY_FAILED:
                    allSucceeded = false;
                    break;
                case COMPLETED:
                    allFailed = false;
                    break;
                default:
                    activeCount++;
                    allCompleted = false;
            }
        }

        if (allCompleted && allSucceeded) {
            return QueryState.COMPLETED;
        } else if (allCompleted && allFailed) {
            return QueryState.FAILED;
        } else if (allCompleted) {
            return QueryState.PARTIALLY_FAILED;
        } else {
            LOGGER.info("Not updating state of parent query, {} leaf queries are still either in progress or queued",
                    activeCount);
            return null;
        }
    }
}
