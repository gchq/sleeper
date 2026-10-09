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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ThreadSleepTestHelper;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.tracker.QueryState;
import sleeper.query.core.tracker.QueryTrackerException;
import sleeper.query.core.tracker.TrackedQuery;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_TRACKER_TABLE_NAME;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.QueryProperty.QUERY_TRACKER_ITEM_TTL_IN_DAYS;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.testutils.JitterTestHelper.constantJitterFraction;
import static sleeper.query.core.tracker.QueryState.COMPLETED;
import static sleeper.query.core.tracker.QueryState.FAILED;
import static sleeper.query.core.tracker.QueryState.IN_PROGRESS;
import static sleeper.query.core.tracker.QueryState.PARTIALLY_FAILED;
import static sleeper.query.core.tracker.QueryState.QUEUED;

public class DynamoDBQueryTrackerIT extends LocalStackTestBase {

    private final InstanceProperties instanceProperties = createInstanceProperties();

    @BeforeEach
    public void createDynamoTable() {
        new DynamoDBQueryTrackerCreator(instanceProperties, dynamoClient).create();
    }

    @Test
    public void shouldReturnNullWhenGettingItemThatDoesNotExist() throws QueryTrackerException {
        // When / Then
        assertThat(queryTracker().getStatus("non-existent")).isNull();
    }

    @Test
    public void shouldReturnQueriesFromDynamoIfTheyExist() throws QueryTrackerException {
        // When
        queryTracker().queryCompleted(createQueryWithId("my-id"), new ResultsOutputInfo(10, Collections.emptyList()));

        // Then
        TrackedQuery status = queryTracker().getStatus("my-id");
        assertThat(status.getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(status.getRowCount()).isEqualTo(Long.valueOf(10));
    }

    @Test
    public void shouldCreateEntryInTableIfIdDoesNotExist() throws QueryTrackerException {
        // When
        queryTracker().queryInProgress(createQueryWithId("my-id"));

        // Then
        assertThat(queryTracker().getStatus("my-id").getLastKnownState()).isEqualTo(IN_PROGRESS);
    }

    @Test
    public void shouldSetAgeOffTimeAccordingToInstanceProperty() throws QueryTrackerException {
        // Given
        instanceProperties.setNumber(QUERY_TRACKER_ITEM_TTL_IN_DAYS, 3);

        // When
        queryTracker().queryInProgress(createQueryWithId("my-id"));
        TrackedQuery status = queryTracker().getStatus("my-id");

        // Then
        assertThat(Instant.ofEpochSecond(status.getExpiryDate()))
                .isEqualTo(Instant.ofEpochMilli(status.getLastUpdateTime())
                        .truncatedTo(ChronoUnit.SECONDS)
                        .plus(Duration.ofDays(3)));
    }

    @Test
    public void shouldReportLastUpdateTimeInMilliseconds() throws QueryTrackerException {
        // Given
        Instant before = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        // When
        queryTracker().queryInProgress(createQueryWithId("my-id"));

        // Then
        TrackedQuery status = queryTracker().getStatus("my-id");
        assertThat(Instant.ofEpochMilli(status.getLastUpdateTime()))
                .isBetween(before, Instant.now());
        assertThat(Instant.ofEpochSecond(status.getExpiryDate()))
                .isAfter(before);
    }

    @Test
    public void shouldUpdateStateInTableIfIdDoesExist() throws QueryTrackerException {
        // When
        queryTracker().queryQueued(createQueryWithId("my-id"));
        queryTracker().queryFailed(createQueryWithId("my-id"), new Exception("fail"));

        // Then
        assertThat(queryTracker().getStatus("my-id").getLastKnownState()).isEqualTo(FAILED);
    }

    @Test
    public void shouldUpdateParentStateInTableWhenTheChildIsTheLastOneToComplete() throws QueryTrackerException {
        // When
        queryTracker().queryInProgress(createQueryWithId("parent"));
        queryTracker().queryCompleted(createSubQueryWithId("parent", "my-id"), new ResultsOutputInfo(10, Collections.emptyList()));

        // Then
        TrackedQuery parent = queryTracker().getStatus("parent");
        TrackedQuery child = queryTracker().getStatus("parent", "my-id");
        assertThat(parent.getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(child.getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(parent.getRowCount()).isEqualTo(Long.valueOf(10));
        assertThat(child.getRowCount()).isEqualTo(Long.valueOf(10));
    }

    @Test
    public void shouldNotReportSubQueryProgressWhenSubQueryCountsWereNotTracked() throws QueryTrackerException {
        // When the sub-queries were not registered with the tracker before running
        queryTracker().queryInProgress(createQueryWithId("parent"));
        queryTracker().queryInProgress(createSubQueryWithId("parent", "my-id"));

        // Then
        TrackedQuery status = queryTracker().getStatus("parent");
        assertThat(status.getExpectedSubQueryCount()).isNull();
        assertThat(status.getFinishedSubQueryCount()).isNull();
        assertThat(status.getRemainingSubQueryCount()).isNull();
        assertThat(status.getFinishedSubQueryRowCount()).isNull();
    }

    @Test
    public void shouldNotUpdateParentStateInTableWhenMoreChildrenAreYetToComplete() throws QueryTrackerException {
        // When
        queryTracker().queryInProgress(createQueryWithId("parent"));
        queryTracker().queryInProgress(createSubQueryWithId("parent", "my-id"));
        queryTracker().queryCompleted(createSubQueryWithId("parent", "my-other-id"), new ResultsOutputInfo(10, Collections.emptyList()));

        // Then
        assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(IN_PROGRESS);
        assertThat(queryTracker().getStatus("parent", "my-other-id").getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(queryTracker().getStatus("parent", "my-id").getLastKnownState()).isEqualTo(IN_PROGRESS);
    }

    @Test
    public void shouldUpdateParentStateToFailedInTableWhenAllChildrenFail() throws QueryTrackerException {
        // When
        queryTracker().queryInProgress(createQueryWithId("parent"));
        queryTracker().queryFailed(createSubQueryWithId("parent", "my-id"), new Exception("Fail"));
        queryTracker().queryFailed(createSubQueryWithId("parent", "my-other-id"), new Exception("Fail"));

        // Then
        assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(FAILED);
        assertThat(queryTracker().getStatus("parent", "my-id").getLastKnownState()).isEqualTo(FAILED);
        assertThat(queryTracker().getStatus("parent", "my-other-id").getLastKnownState()).isEqualTo(FAILED);
    }

    @Test
    public void shouldUpdateParentStateToPartiallyFailedInTableWhenSomeChildrenFail() throws QueryTrackerException {
        // When
        queryTracker().queryInProgress(createQueryWithId("parent"));
        queryTracker().queryCompleted(createSubQueryWithId("parent", "my-id"), new ResultsOutputInfo(10, Collections.emptyList()));
        queryTracker().queryFailed(createSubQueryWithId("parent", "my-other-id"), new Exception("Fail"));

        // Then
        assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(PARTIALLY_FAILED);
        assertThat(queryTracker().getStatus("parent", "my-id").getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(queryTracker().getStatus("parent", "my-other-id").getLastKnownState()).isEqualTo(FAILED);
    }

    @Test
    public void shouldUpdateParentStateInTableWhenTheLastChildToFinishPartiallyFailed() throws QueryTrackerException {
        // When
        queryTracker().queryInProgress(createQueryWithId("parent"));
        queryTracker().queryInProgress(createSubQueryWithId("parent", "my-id"));
        queryTracker().queryInProgress(createSubQueryWithId("parent", "my-other-id"));
        queryTracker().queryCompleted(createSubQueryWithId("parent", "my-id"), new ResultsOutputInfo(10, Collections.emptyList()));
        queryTracker().queryCompleted(createSubQueryWithId("parent", "my-other-id"),
                new ResultsOutputInfo(5, Collections.emptyList(), new Exception("Failed part way through")));

        // Then
        assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(PARTIALLY_FAILED);
        assertThat(queryTracker().getStatus("parent").getRowCount()).isEqualTo(Long.valueOf(15));
        assertThat(queryTracker().getStatus("parent", "my-id").getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(queryTracker().getStatus("parent", "my-other-id").getLastKnownState()).isEqualTo(PARTIALLY_FAILED);
    }

    @Test
    public void shouldUpdateParentStateWithTotalRowsReturnedByAllChildren() throws QueryTrackerException {
        // When
        queryTracker().queryInProgress(createQueryWithId("parent"));
        queryTracker().queryCompleted(createSubQueryWithId("parent", "my-id"), new ResultsOutputInfo(10, Collections.emptyList()));
        queryTracker().queryCompleted(createSubQueryWithId("parent", "my-other-id"), new ResultsOutputInfo(25, Collections.emptyList()));

        // Then
        assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(queryTracker().getStatus("parent", "my-id").getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(queryTracker().getStatus("parent", "my-other-id").getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(queryTracker().getStatus("parent").getRowCount()).isEqualTo(Long.valueOf(35));
        assertThat(queryTracker().getStatus("parent", "my-id").getRowCount()).isEqualTo(Long.valueOf(10));
        assertThat(queryTracker().getStatus("parent", "my-other-id").getRowCount()).isEqualTo(Long.valueOf(25));
    }

    @Test
    public void shouldTrackCreationOfSubQueries() throws QueryTrackerException {
        // Given
        Query parent = createQueryWithId("parent");
        queryTracker().queryInProgress(parent);

        // When
        queryTracker().subQueriesCreated(parent, List.of(
                createSubQueryWithId("parent", "sub-1"),
                createSubQueryWithId("parent", "sub-2")));

        // Then
        // The parent records how many subqueries to expect, and the subqueries are only tracked
        // individually once they start running
        TrackedQuery parentStatus = queryTracker().getStatus("parent");
        assertThat(parentStatus.getLastKnownState()).isEqualTo(IN_PROGRESS);
        assertThat(parentStatus.getExpectedSubQueryCount()).isEqualTo(2L);
        assertThat(parentStatus.getRemainingSubQueryCount()).isEqualTo(2L);
        assertThat(queryTracker().getStatus("parent", "sub-1")).isNull();
        assertThat(queryTracker().getStatus("parent", "sub-2")).isNull();
    }

    @Test
    public void shouldTrackSubQueryWhenItStartsRunning() throws QueryTrackerException {
        // Given
        Query parent = createQueryWithId("parent");
        LeafPartitionQuery subQuery = createSubQueryWithId("parent", "sub-1");
        queryTracker().queryInProgress(parent);
        queryTracker().subQueriesCreated(parent, List.of(subQuery));

        // When
        queryTracker().queryInProgress(subQuery);

        // Then
        assertThat(queryTracker().getStatus("parent", "sub-1").getLastKnownState()).isEqualTo(IN_PROGRESS);
    }

    @Test
    public void shouldCountSubQueriesAcrossManyCounterShards() throws QueryTrackerException {
        // Given more subqueries than counter shards, so that every shard holds part of the counts
        Query parent = createQueryWithId("parent");
        List<LeafPartitionQuery> subQueries = IntStream.rangeClosed(1, 40)
                .mapToObj(i -> createSubQueryWithId("parent", "sub-" + i))
                .toList();
        queryTracker().queryInProgress(parent);
        queryTracker().subQueriesCreated(parent, subQueries);

        // When all but one subquery completes, returning 1, 2, 3... rows
        for (int i = 0; i < 39; i++) {
            queryTracker().queryCompleted(subQueries.get(i), new ResultsOutputInfo(i + 1, Collections.emptyList()));
        }

        // Then the counts are summed across the shards
        TrackedQuery inProgress = queryTracker().getStatus("parent");
        assertThat(inProgress.getLastKnownState()).isEqualTo(IN_PROGRESS);
        assertThat(inProgress.getSucceededSubQueryCount()).isEqualTo(39L);
        assertThat(inProgress.getRemainingSubQueryCount()).isEqualTo(1L);

        // And the last completion finishes the parent with the rows totalled across the shards
        queryTracker().queryCompleted(subQueries.get(39), new ResultsOutputInfo(40, Collections.emptyList()));
        assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(queryTracker().getStatus("parent").getRowCount()).isEqualTo(Long.valueOf(820));

        // And the counter shard items carry an expiry date for the table's time to live setting
        Map<String, AttributeValue> shardItem = dynamoClient.getItem(request -> request
                .tableName(instanceProperties.get(QUERY_TRACKER_TABLE_NAME))
                .key(Map.of(
                        DynamoDBQueryTracker.QUERY_ID, AttributeValue.fromS("parent"),
                        DynamoDBQueryTracker.SUB_QUERY_ID, AttributeValue.fromS(DynamoDBQueryTrackerEntry.counterShardSortKey(0)))))
                .item();
        assertThat(shardItem).containsKey("expiryDate");
    }

    @Nested
    @DisplayName("Update parent query from counters of finished sub-queries")
    class UpdateParentFromCounters {
        List<QueryRequest> queryRequests = new ArrayList<>();
        List<Duration> foundWaits = new ArrayList<>();
        Query parent = createQueryWithId("parent");
        LeafPartitionQuery sub1 = createSubQueryWithId("parent", "sub-1");
        LeafPartitionQuery sub2 = createSubQueryWithId("parent", "sub-2");

        @BeforeEach
        void setUp() {
            queryTracker().queryInProgress(parent);
            queryTracker().subQueriesCreated(parent, List.of(sub1, sub2));
        }

        @Test
        void shouldReportProgressBeforeAnySubQueryFinishes() throws QueryTrackerException {
            // When
            TrackedQuery status = queryTracker().getStatus("parent");

            // Then
            assertThat(status.getExpectedSubQueryCount()).isEqualTo(2L);
            assertThat(status.getFinishedSubQueryCount()).isEqualTo(0L);
            assertThat(status.getRemainingSubQueryCount()).isEqualTo(2L);
            assertThat(status.getFinishedSubQueryRowCount()).isEqualTo(0L);
        }

        @Test
        void shouldReportProgressWhenSomeSubQueriesHaveFinished() throws QueryTrackerException {
            // When
            queryTracker().queryCompleted(sub1, new ResultsOutputInfo(10, Collections.emptyList()));

            // Then
            TrackedQuery status = queryTracker().getStatus("parent");
            assertThat(status.getExpectedSubQueryCount()).isEqualTo(2L);
            assertThat(status.getSucceededSubQueryCount()).isEqualTo(1L);
            assertThat(status.getFailedSubQueryCount()).isEqualTo(0L);
            assertThat(status.getFinishedSubQueryCount()).isEqualTo(1L);
            assertThat(status.getRemainingSubQueryCount()).isEqualTo(1L);
            assertThat(status.getFinishedSubQueryRowCount()).isEqualTo(10L);
        }

        @Test
        void shouldReportProgressWhenASubQueryFailed() throws QueryTrackerException {
            // When
            queryTracker().queryFailed(sub1, new Exception("Fail"));

            // Then
            TrackedQuery status = queryTracker().getStatus("parent");
            assertThat(status.getExpectedSubQueryCount()).isEqualTo(2L);
            assertThat(status.getSucceededSubQueryCount()).isEqualTo(0L);
            assertThat(status.getFailedSubQueryCount()).isEqualTo(1L);
            assertThat(status.getFinishedSubQueryCount()).isEqualTo(1L);
            assertThat(status.getRemainingSubQueryCount()).isEqualTo(1L);
        }

        @Test
        void shouldCompleteParentWithoutReadingSubQueryItems() throws QueryTrackerException {
            // Given
            DynamoDBQueryTracker tracker = trackerRecordingQueryRequests();

            // When
            tracker.queryCompleted(sub1, new ResultsOutputInfo(10, Collections.emptyList()));
            tracker.queryCompleted(sub2, new ResultsOutputInfo(25, Collections.emptyList()));

            // Then
            // The subquery items are not read
            assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(COMPLETED);
            assertThat(queryTracker().getStatus("parent").getRowCount()).isEqualTo(Long.valueOf(35));
            assertThat(queryRequests).isNotEmpty().allSatisfy(request -> assertThat(request.keyConditionExpression())
                    .contains("begins_with"));
        }

        @Test
        void shouldNotFinishParentWhenNotAllSubQueriesHaveFinished() throws QueryTrackerException {
            // When
            queryTracker().queryCompleted(sub1, new ResultsOutputInfo(10, Collections.emptyList()));

            // Then
            assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(IN_PROGRESS);
        }

        @Test
        void shouldFailParentWhenAllSubQueriesFailed() throws QueryTrackerException {
            // When
            queryTracker().queryFailed(sub1, new Exception("Fail"));
            queryTracker().queryFailed(sub2, new Exception("Fail"));

            // Then
            assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(FAILED);
        }

        @Test
        void shouldPartiallyFailParentWhenSomeSubQueriesFailed() throws QueryTrackerException {
            // When
            queryTracker().queryCompleted(sub1, new ResultsOutputInfo(10, Collections.emptyList()));
            queryTracker().queryFailed(sub2, new Exception("Fail"));

            // Then
            assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(PARTIALLY_FAILED);
            assertThat(queryTracker().getStatus("parent").getRowCount()).isEqualTo(Long.valueOf(10));
        }

        @Test
        void shouldNotCountSubQueryTwiceWhenItIsCompletedTwice() throws QueryTrackerException {
            // When a duplicate message completes the same sub-query twice
            queryTracker().queryCompleted(sub1, new ResultsOutputInfo(10, Collections.emptyList()));
            queryTracker().queryCompleted(sub1, new ResultsOutputInfo(10, Collections.emptyList()));

            // Then the parent is still waiting for the other sub-query
            assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(IN_PROGRESS);

            // And when the other sub-query completes, the parent finishes with each sub-query counted once
            queryTracker().queryCompleted(sub2, new ResultsOutputInfo(5, Collections.emptyList()));
            assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(COMPLETED);
            assertThat(queryTracker().getStatus("parent").getRowCount()).isEqualTo(Long.valueOf(15));
        }

        @Test
        void shouldNotRevertFinishedSubQueryWhenDuplicateMessageRestartsIt() throws QueryTrackerException {
            // When a duplicate message restarts a sub-query that already finished
            queryTracker().queryCompleted(sub1, new ResultsOutputInfo(10, Collections.emptyList()));
            queryTracker().queryInProgress(sub1);

            // Then the sub-query is still finished
            assertThat(queryTracker().getStatus("parent", "sub-1").getLastKnownState()).isEqualTo(COMPLETED);

            // And when the other sub-query completes, the parent finishes with each sub-query counted once
            queryTracker().queryCompleted(sub2, new ResultsOutputInfo(5, Collections.emptyList()));
            assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(COMPLETED);
            assertThat(queryTracker().getStatus("parent").getRowCount()).isEqualTo(Long.valueOf(15));
        }

        @Test
        void shouldCountSubQueryOnRetryWhenFirstCompletionFailed() throws QueryTrackerException {
            // Given
            // First transaction fails
            DynamoDBQueryTracker tracker = trackerFailingFirstTransaction();
            assertThatThrownBy(() -> tracker.queryCompleted(sub1, new ResultsOutputInfo(10, Collections.emptyList())))
                    .isInstanceOf(ProvisionedThroughputExceededException.class);
            // And the subquery was not marked as finished, so a retry can still count it
            assertThat(queryTracker().getStatus("parent", "sub-1")).isNull();

            // When the completion is retried and the other subquery completes
            tracker.queryCompleted(sub1, new ResultsOutputInfo(10, Collections.emptyList()));
            tracker.queryCompleted(sub2, new ResultsOutputInfo(5, Collections.emptyList()));

            // Then each subquery was counted exactly once
            assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(COMPLETED);
            assertThat(queryTracker().getStatus("parent").getRowCount()).isEqualTo(Long.valueOf(15));
        }

        @Test
        void shouldRetryCountingSubQueryWhenTransactionConflicts() throws QueryTrackerException {
            // Given counting a finished subquery conflicts twice with other transactions on the parent query's item
            DynamoDBQueryTracker tracker = trackerRetryingConflicts(conflictingTransactions(2));

            // When
            tracker.queryCompleted(sub1, new ResultsOutputInfo(10, Collections.emptyList()));
            tracker.queryCompleted(sub2, new ResultsOutputInfo(5, Collections.emptyList()));

            // Then the subquery was counted after retrying with backoff
            assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(COMPLETED);
            assertThat(queryTracker().getStatus("parent").getRowCount()).isEqualTo(Long.valueOf(15));
            assertThat(foundWaits).containsExactly(Duration.ofMillis(500), Duration.ofSeconds(1));
        }

        @Test
        void shouldFailCountingSubQueryWhenTransactionConflictsTooManyTimes() throws QueryTrackerException {
            // Given counting a finished subquery conflicts on every attempt
            DynamoDBQueryTracker tracker = trackerRetryingConflicts(conflictingTransactions(Integer.MAX_VALUE));

            // When / Then
            assertThatThrownBy(() -> tracker.queryCompleted(sub1, new ResultsOutputInfo(10, Collections.emptyList())))
                    .isInstanceOf(TransactionCanceledException.class);
            assertThat(foundWaits).hasSize(9);

            // And the subquery was not marked as finished, so a retry can still count it
            assertThat(queryTracker().getStatus("parent", "sub-1")).isNull();
            assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(IN_PROGRESS);
        }

        @Test
        void shouldFinishParentOnDuplicateCompletionWhenFinishingParentWasLost() throws QueryTrackerException {
            // Given all subqueries finished but the write finishing the parent query was lost
            queryTracker().queryCompleted(sub1, new ResultsOutputInfo(10, Collections.emptyList()));
            queryTracker().queryCompleted(sub2, new ResultsOutputInfo(25, Collections.emptyList()));
            setParentState("parent", IN_PROGRESS);

            // When a duplicate message completes a subquery again
            queryTracker().queryCompleted(sub2, new ResultsOutputInfo(25, Collections.emptyList()));

            // Then the duplicate is not counted, and the parent query is finished from the counters
            assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(COMPLETED);
            assertThat(queryTracker().getStatus("parent").getRowCount()).isEqualTo(Long.valueOf(35));
        }

        private void setParentState(String queryId, QueryState state) {
            dynamoClient.updateItem(request -> request
                    .tableName(instanceProperties.get(QUERY_TRACKER_TABLE_NAME))
                    .key(Map.of(
                            DynamoDBQueryTracker.QUERY_ID, AttributeValue.fromS(queryId),
                            DynamoDBQueryTracker.SUB_QUERY_ID, AttributeValue.fromS(DynamoDBQueryTracker.NON_NESTED_QUERY_PLACEHOLDER)))
                    .updateExpression("SET #State = :state")
                    .expressionAttributeNames(Map.of("#State", DynamoDBQueryTracker.LAST_KNOWN_STATE))
                    .expressionAttributeValues(Map.of(":state", AttributeValue.fromS(state.name()))));
        }

        @Test
        void shouldNotCountSubQueryFinishingUnderAPreviousAttempt() throws QueryTrackerException {
            // Given the query was registered and partly processed under a first attempt
            queryTracker().subQueriesCreated(parent, List.of(
                    sub1.withAttemptId("attempt-1"), sub2.withAttemptId("attempt-1")));
            queryTracker().queryCompleted(sub1.withAttemptId("attempt-1"), new ResultsOutputInfo(10, Collections.emptyList()));

            // When the whole query is reprocessed as a new attempt, then a late completion from the first
            // attempt arrives
            queryTracker().subQueriesCreated(parent, List.of(
                    sub1.withAttemptId("attempt-2"), sub2.withAttemptId("attempt-2")));
            queryTracker().queryCompleted(sub2.withAttemptId("attempt-1"), new ResultsOutputInfo(25, Collections.emptyList()));

            // Then the late completion is neither counted nor recorded against the subquery
            assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(IN_PROGRESS);
            assertThat(queryTracker().getStatus("parent").getFinishedSubQueryCount()).isEqualTo(0L);
            assertThat(queryTracker().getStatus("parent", "sub-2")).isNull();

            // And the new attempt's completions finish the parent with each subquery counted once
            queryTracker().queryCompleted(sub1.withAttemptId("attempt-2"), new ResultsOutputInfo(10, Collections.emptyList()));
            queryTracker().queryCompleted(sub2.withAttemptId("attempt-2"), new ResultsOutputInfo(25, Collections.emptyList()));
            assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(COMPLETED);
            assertThat(queryTracker().getStatus("parent").getRowCount()).isEqualTo(Long.valueOf(35));
        }

        @Test
        void shouldHideCounterShardsFromListingsAndSumCountersIntoParent() throws QueryTrackerException {
            // When
            queryTracker().queryInProgress(sub1);
            queryTracker().queryCompleted(sub2, new ResultsOutputInfo(25, Collections.emptyList()));

            // Then the listings show only the parent and subquery entries
            assertThat(queryTracker().getAllQueries())
                    .extracting(TrackedQuery::getSubQueryId, TrackedQuery::getLastKnownState)
                    .containsExactlyInAnyOrder(
                            tuple("-", IN_PROGRESS),
                            tuple("sub-1", IN_PROGRESS),
                            tuple("sub-2", COMPLETED));
            assertThat(queryTracker().getQueriesWithState(IN_PROGRESS))
                    .extracting(TrackedQuery::getSubQueryId)
                    .containsExactlyInAnyOrder("-", "sub-1");
            assertThat(queryTracker().getFailedQueries()).isEmpty();

            // And the counters are summed into the parent's entry
            assertThat(queryTracker().getAllQueries())
                    .filteredOn(query -> "-".equals(query.getSubQueryId()))
                    .extracting(TrackedQuery::getExpectedSubQueryCount, TrackedQuery::getSucceededSubQueryCount,
                            TrackedQuery::getFinishedSubQueryRowCount)
                    .containsExactly(tuple(2L, 1L, 25L));
        }

        @Test
        void shouldSumCountersIntoParentWhenListingQueriesWithState() throws QueryTrackerException {
            // When
            queryTracker().queryInProgress(sub1);
            queryTracker().queryCompleted(sub2, new ResultsOutputInfo(25, Collections.emptyList()));

            // Then
            assertThat(queryTracker().getQueriesWithState(IN_PROGRESS))
                    .filteredOn(query -> "-".equals(query.getSubQueryId()))
                    .extracting(TrackedQuery::getExpectedSubQueryCount, TrackedQuery::getSucceededSubQueryCount,
                            TrackedQuery::getFinishedSubQueryRowCount)
                    .containsExactly(tuple(2L, 1L, 25L));
        }

        @Test
        void shouldSumCountersIntoParentWhenListingFailedQueries() throws QueryTrackerException {
            // When
            queryTracker().queryFailed(sub1, new Exception("Fail"));
            queryTracker().queryFailed(sub2, new Exception("Fail"));

            // Then
            assertThat(queryTracker().getFailedQueries())
                    .filteredOn(query -> "-".equals(query.getSubQueryId()))
                    .extracting(TrackedQuery::getLastKnownState, TrackedQuery::getExpectedSubQueryCount,
                            TrackedQuery::getFailedSubQueryCount)
                    .containsExactly(tuple(FAILED, 2L, 2L));
        }

        @Test
        void shouldResetCountersWhenSubQueriesAreRecreatedByDuplicateOfParentQuery() throws QueryTrackerException {
            // Given the query ran once already
            queryTracker().queryCompleted(sub1.withAttemptId("attempt-1"), new ResultsOutputInfo(10, Collections.emptyList()));
            queryTracker().queryCompleted(sub2.withAttemptId("attempt-1"), new ResultsOutputInfo(25, Collections.emptyList()));

            // When a duplicate message reruns the whole query as a new attempt
            queryTracker().queryInProgress(parent);
            queryTracker().subQueriesCreated(parent, List.of(sub1, sub2));
            queryTracker().queryInProgress(sub1.withAttemptId("attempt-2"));
            queryTracker().queryCompleted(sub1.withAttemptId("attempt-2"), new ResultsOutputInfo(10, Collections.emptyList()));
            queryTracker().queryCompleted(sub2.withAttemptId("attempt-2"), new ResultsOutputInfo(25, Collections.emptyList()));

            // Then
            assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(COMPLETED);
            assertThat(queryTracker().getStatus("parent").getRowCount()).isEqualTo(Long.valueOf(35));
        }

        @Test
        void shouldNotCountSubQueryTwiceWhenItIsCompletedTwiceInTheSameRun() throws QueryTrackerException {
            // When a duplicate message completes the same subquery twice in the same attempt
            queryTracker().queryCompleted(sub1.withAttemptId("attempt-1"), new ResultsOutputInfo(10, Collections.emptyList()));
            queryTracker().queryCompleted(sub1.withAttemptId("attempt-1"), new ResultsOutputInfo(10, Collections.emptyList()));

            // Then the parent is still waiting for the other subquery
            assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(IN_PROGRESS);

            // And when the other subquery completes, the parent finishes with each subquery counted once
            queryTracker().queryCompleted(sub2.withAttemptId("attempt-1"), new ResultsOutputInfo(5, Collections.emptyList()));
            assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(COMPLETED);
            assertThat(queryTracker().getStatus("parent").getRowCount()).isEqualTo(Long.valueOf(15));
        }

        private DynamoDBQueryTracker trackerRecordingQueryRequests() {
            return new DynamoDBQueryTracker(instanceProperties, new DynamoDbClient() {

                @Override
                public QueryResponse query(QueryRequest request) {
                    queryRequests.add(request);
                    return dynamoClient.query(request);
                }

                @Override
                public UpdateItemResponse updateItem(UpdateItemRequest request) {
                    return dynamoClient.updateItem(request);
                }

                @Override
                public TransactWriteItemsResponse transactWriteItems(TransactWriteItemsRequest request) {
                    return dynamoClient.transactWriteItems(request);
                }

                @Override
                public GetItemResponse getItem(GetItemRequest request) {
                    return dynamoClient.getItem(request);
                }

                @Override
                public String serviceName() {
                    return dynamoClient.serviceName();
                }

                @Override
                public void close() {
                }
            });
        }

        private DynamoDBQueryTracker trackerRetryingConflicts(DynamoDbClient client) {
            return new DynamoDBQueryTracker(instanceProperties, client,
                    new ExponentialBackoffWithJitter(DynamoDBQueryTracker.TRANSACTION_CONFLICT_WAIT_RANGE,
                            constantJitterFraction(0.5), ThreadSleepTestHelper.recordWaits(foundWaits)));
        }

        private DynamoDbClient conflictingTransactions(int conflicts) {
            AtomicInteger transactions = new AtomicInteger(0);
            return new DynamoDbClient() {

                @Override
                public TransactWriteItemsResponse transactWriteItems(TransactWriteItemsRequest request) {
                    if (transactions.incrementAndGet() <= conflicts) {
                        throw TransactionCanceledException.builder()
                                .message("Transaction cancelled, please refer cancellation reasons for specific reasons " +
                                        "[None, TransactionConflict]")
                                .cancellationReasons(
                                        CancellationReason.builder().code("None").build(),
                                        CancellationReason.builder().code("TransactionConflict").build())
                                .build();
                    }
                    return dynamoClient.transactWriteItems(request);
                }

                @Override
                public UpdateItemResponse updateItem(UpdateItemRequest request) {
                    return dynamoClient.updateItem(request);
                }

                @Override
                public GetItemResponse getItem(GetItemRequest request) {
                    return dynamoClient.getItem(request);
                }

                @Override
                public QueryResponse query(QueryRequest request) {
                    return dynamoClient.query(request);
                }

                @Override
                public String serviceName() {
                    return dynamoClient.serviceName();
                }

                @Override
                public void close() {
                }
            };
        }

        private DynamoDBQueryTracker trackerFailingFirstTransaction() {
            AtomicBoolean failed = new AtomicBoolean(false);
            return new DynamoDBQueryTracker(instanceProperties, new DynamoDbClient() {

                @Override
                public TransactWriteItemsResponse transactWriteItems(TransactWriteItemsRequest request) {
                    if (failed.compareAndSet(false, true)) {
                        throw ProvisionedThroughputExceededException.builder()
                                .message("Fake failure applying transaction")
                                .build();
                    }
                    return dynamoClient.transactWriteItems(request);
                }

                @Override
                public UpdateItemResponse updateItem(UpdateItemRequest request) {
                    return dynamoClient.updateItem(request);
                }

                @Override
                public GetItemResponse getItem(GetItemRequest request) {
                    return dynamoClient.getItem(request);
                }

                @Override
                public QueryResponse query(QueryRequest request) {
                    return dynamoClient.query(request);
                }

                @Override
                public String serviceName() {
                    return dynamoClient.serviceName();
                }

                @Override
                public void close() {
                }
            });
        }
    }

    @Test
    void shouldStoreErrorMessageWhenQueryFailed() {
        // When
        queryTracker().queryFailed(createQueryWithId("failed-query"), new Exception("Query has failed"));

        // Then
        assertThat(queryTracker().getAllQueries())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastUpdateTime", "expiryDate")
                .containsExactly(TrackedQuery.builder()
                        .queryId("failed-query")
                        .lastKnownState(FAILED)
                        .errorMessage("Query has failed").build());
    }

    @Test
    void shouldStoreErrorMessageWhenQueryCompletedWithError() {
        // When
        queryTracker().queryCompleted(createQueryWithId("completed-query-that-errored"),
                new ResultsOutputInfo(100L, List.of(), new Exception("Query has failed")));

        // Then
        assertThat(queryTracker().getAllQueries())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastUpdateTime", "expiryDate")
                .containsExactly(TrackedQuery.builder()
                        .queryId("completed-query-that-errored")
                        .lastKnownState(PARTIALLY_FAILED)
                        .rowCount(100L)
                        .errorMessage("Query has failed").build());
    }

    @Nested
    @DisplayName("Get tracked queries")
    class GetTrackedQueries {
        Query query1 = createQueryWithId("test-query-1");
        Query query2 = createQueryWithId("test-query-2");
        Query query3 = createQueryWithId("test-query-3");
        Query query4 = createQueryWithId("test-query-4");
        Query query5 = createQueryWithId("test-query-5");

        @BeforeEach
        void setUp() {
            queryTracker().queryQueued(query1);
            queryTracker().queryInProgress(query2);
            queryTracker().queryCompleted(query3, new ResultsOutputInfo(456L, List.of()));
            queryTracker().queryFailed(query4, new Exception("Failed"));
            queryTracker().queryCompleted(query5, new ResultsOutputInfo(123L, List.of(), new Exception("Partially failed")));
        }

        @Test
        void shouldGetAllQueries() {
            // When / Then
            assertThat(queryTracker().getAllQueries())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("expiryDate", "lastUpdateTime")
                    .containsExactlyInAnyOrder(
                            queryQueued(query1),
                            queryInProgress(query2),
                            queryCompleted(query3, 456L),
                            queryFailed(query4, "Failed"),
                            queryPartiallyFailed(query5, 123L, "Partially failed"));
        }

        @Test
        void shouldGetPendingQueries() {
            // When / Then
            assertThat(queryTracker().getQueriesWithState(QUEUED))
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("expiryDate", "lastUpdateTime")
                    .containsExactly(queryQueued(query1));
        }

        @Test
        void shouldGetInProgressQueries() {
            // When / Then
            assertThat(queryTracker().getQueriesWithState(IN_PROGRESS))
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("expiryDate", "lastUpdateTime")
                    .containsExactlyInAnyOrder(queryInProgress(query2));
        }

        @Test
        void shouldGetCompletedQueries() {
            // When / Then
            assertThat(queryTracker().getQueriesWithState(COMPLETED))
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("expiryDate", "lastUpdateTime")
                    .containsExactlyInAnyOrder(queryCompleted(query3, 456L));
        }

        @Test
        void shouldGetFailedQueries() {
            // When / Then
            assertThat(queryTracker().getFailedQueries())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("expiryDate", "lastUpdateTime")
                    .containsExactlyInAnyOrder(
                            queryFailed(query4, "Failed"),
                            queryPartiallyFailed(query5, 123L, "Partially failed"));
        }
    }

    @Nested
    @DisplayName("Paginate through tracked queries when they exceed one page")
    class PaginateResults {
        // Roughly 350KB - 4 or more of these will exceed 1MB
        String largeErrorMessage = "x".repeat(350 * 1024);

        @Test
        void shouldGetAllQueriesWhenTheyExceedOnePage() {
            // Given
            // 5 of these large error messages will exceed 1MB
            for (int i = 1; i <= 5; i++) {
                queryTracker().queryFailed(createQueryWithId("query-" + i), new Exception(largeErrorMessage));
            }

            // When / Then
            assertThat(countPagesInScanOfTracker()).isGreaterThan(1);
            assertThat(queryTracker().getAllQueries())
                    .extracting(TrackedQuery::getQueryId)
                    .containsExactlyInAnyOrder("query-1", "query-2", "query-3", "query-4", "query-5");
            assertThat(queryTracker().getQueriesWithState(FAILED)).hasSize(5);
            assertThat(queryTracker().getFailedQueries()).hasSize(5);
        }

        @Test
        void shouldNotFinishParentWhenUnfinishedChildIsBeyondFirstPageOfChildren() throws QueryTrackerException {
            // Given a child that is still running, which sorts after more than one page of finished children
            queryTracker().queryInProgress(createQueryWithId("parent"));
            queryTracker().queryInProgress(createSubQueryWithId("parent", "z-still-running"));

            // When
            // 4 of these exceed 1MB
            for (int i = 1; i <= 4; i++) {
                queryTracker().queryFailed(createSubQueryWithId("parent", "child-" + i), new Exception(largeErrorMessage));
            }

            // Then
            assertThat(countPagesInQueryForId("parent")).isGreaterThan(1);
            assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(IN_PROGRESS);
            assertThat(queryTracker().getStatus("parent", "z-still-running").getLastKnownState()).isEqualTo(IN_PROGRESS);
        }

        @Test
        void shouldFinishParentWhenChildrenExceedOnePage() throws QueryTrackerException {
            // Given
            queryTracker().queryInProgress(createQueryWithId("parent"));
            queryTracker().queryInProgress(createSubQueryWithId("parent", "z-last-child"));

            // When
            // 4 of these exceed 1MB
            for (int i = 1; i <= 4; i++) {
                queryTracker().queryFailed(createSubQueryWithId("parent", "child-" + i), new Exception(largeErrorMessage));
            }
            queryTracker().queryCompleted(createSubQueryWithId("parent", "z-last-child"), new ResultsOutputInfo(10, Collections.emptyList()));

            // Then
            assertThat(countPagesInQueryForId("parent")).isGreaterThan(1);
            assertThat(queryTracker().getStatus("parent").getLastKnownState()).isEqualTo(PARTIALLY_FAILED);
            assertThat(queryTracker().getStatus("parent").getRowCount()).isEqualTo(Long.valueOf(10));
        }

        private long countPagesInScanOfTracker() {
            return dynamoClient.scanPaginator(request -> request
                    .tableName(instanceProperties.get(QUERY_TRACKER_TABLE_NAME)))
                    .stream().count();
        }

        private long countPagesInQueryForId(String queryId) {
            return dynamoClient.queryPaginator(request -> request
                    .tableName(instanceProperties.get(QUERY_TRACKER_TABLE_NAME))
                    .keyConditionExpression("#QueryId = :queryId")
                    .expressionAttributeNames(Map.of("#QueryId", DynamoDBQueryTracker.QUERY_ID))
                    .expressionAttributeValues(Map.of(":queryId", AttributeValue.fromS(queryId))))
                    .stream().count();
        }
    }

    private DynamoDBQueryTracker queryTracker() {
        return new DynamoDBQueryTracker(instanceProperties, dynamoClient);
    }

    private TrackedQuery queryQueued(Query query) {
        return TrackedQueryTestHelper.queryQueued(query.getQueryId(), Instant.now());
    }

    private TrackedQuery queryInProgress(Query query) {
        return TrackedQueryTestHelper.queryInProgress(query.getQueryId(), Instant.now());
    }

    private TrackedQuery queryCompleted(Query query, long rows) {
        return TrackedQueryTestHelper.queryCompleted(query.getQueryId(), Instant.now(), rows);
    }

    private TrackedQuery queryFailed(Query query, String errorMessage) {
        return TrackedQueryTestHelper.queryFailed(query.getQueryId(), Instant.now(), errorMessage);
    }

    private TrackedQuery queryPartiallyFailed(Query query, long rows, String errorMessage) {
        return TrackedQueryTestHelper.queryPartiallyFailed(query.getQueryId(), Instant.now(), rows, errorMessage);
    }

    private Query createQueryWithId(String id) {
        Field field = new Field("field1", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range = rangeFactory.createExactRange(field, 1);
        Region region = new Region(range);
        return Query.builder()
                .tableName("myTable")
                .queryId(id)
                .regions(List.of(region))
                .build();
    }

    private LeafPartitionQuery createSubQueryWithId(String parentId, String subId) {
        Field field = new Field("field1", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range = rangeFactory.createExactRange(field, 1);
        Region region = new Region(range);
        Range partitionRange = rangeFactory.createRange(field, 0, 1000);
        Region partitionRegion = new Region(partitionRange);
        Query query = Query.builder()
                .tableName("myTable")
                .queryId(parentId)
                .regions(List.of(region))
                .build();
        return LeafPartitionQuery.builder()
                .parentQuery(query)
                .tableId("myTableId")
                .subQueryId(subId)
                .regions(List.of(region))
                .leafPartitionId("leafId")
                .partitionRegion(partitionRegion)
                .files(List.of())
                .build();
    }

    private static InstanceProperties createInstanceProperties() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(QUERY_TRACKER_TABLE_NAME, instanceProperties.get(ID) + "-query-tracker");
        return instanceProperties;
    }
}
