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
package sleeper.systemtest.drivers.statestore;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.QueryStatus;

import sleeper.core.util.PollWithRetries;
import sleeper.core.util.SplitIntoBatches;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.statestore.StateStoreCommitMessage;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterDriver;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterRun;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_LOG_GROUP;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;

public class AwsStateStoreCommitterDriver implements StateStoreCommitterDriver {
    public static final Logger LOGGER = LoggerFactory.getLogger(AwsStateStoreCommitterDriver.class);

    private final SystemTestInstanceContext instance;
    private final AmazonSQS sqs;
    private final CloudWatchLogsClient cloudWatch;

    public AwsStateStoreCommitterDriver(SystemTestInstanceContext instance, AmazonSQS sqs, CloudWatchLogsClient cloudWatch) {
        this.instance = instance;
        this.sqs = sqs;
        this.cloudWatch = cloudWatch;
    }

    @Override
    public void sendCommitMessages(Stream<StateStoreCommitMessage> messages) {
        SplitIntoBatches.inParallelBatchesOf(10, messages, this::sendMessageBatch);
    }

    @Override
    public List<StateStoreCommitterRun> getRunsInPeriod(Instant startTime, Instant endTime) {
        String logGroupName = instance.getInstanceProperties().get(STATESTORE_COMMITTER_LOG_GROUP);
        LOGGER.info("Submitting logs query for log group {} starting at time {}", logGroupName, startTime);
        String queryId = cloudWatch.startQuery(builder -> builder
                .logGroupName(logGroupName)
                .startTime(startTime.getEpochSecond())
                .endTime(endTime.getEpochSecond())
                .limit(10000)
                .queryString("fields @timestamp, @message, @logStream " +
                        "| filter @message like /Lambda (started|finished) at|Applied request to table/ " +
                        "| sort @timestamp asc"))
                .queryId();
        GetQueryResultsResponse response = waitForQuery(queryId);
        StateStoreCommitterRunsBuilder builder = new StateStoreCommitterRunsBuilder();
        response.results().forEach(builder::add);
        return builder.buildRuns();
    }

    private void sendMessageBatch(List<StateStoreCommitMessage> batch) {
        sqs.sendMessageBatch(new SendMessageBatchRequest()
                .withQueueUrl(instance.getInstanceProperties().get(STATESTORE_COMMITTER_QUEUE_URL))
                .withEntries(batch.stream()
                        .map(message -> new SendMessageBatchRequestEntry()
                                .withMessageDeduplicationId(UUID.randomUUID().toString())
                                .withId(UUID.randomUUID().toString())
                                .withMessageGroupId(message.getTableId())
                                .withMessageBody(message.getBody()))
                        .collect(toUnmodifiableList())));
    }

    private GetQueryResultsResponse waitForQuery(String queryId) {
        try {
            return PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(1), Duration.ofMinutes(1))
                    .queryUntil("query is completed",
                            () -> cloudWatch.getQueryResults(builder -> builder.queryId(queryId)),
                            results -> isQueryCompleted(results));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static boolean isQueryCompleted(GetQueryResultsResponse response) {
        LOGGER.info("Logs query response status {}, statistics: {}",
                response.statusAsString(), response.statistics());
        QueryStatus status = response.status();
        if (status == QueryStatus.COMPLETE) {
            return true;
        } else if (Set.of(QueryStatus.SCHEDULED, QueryStatus.RUNNING).contains(status)) {
            return false;
        } else {
            throw new RuntimeException("Logs query failed with status " + response.statusAsString());
        }
    }

}
