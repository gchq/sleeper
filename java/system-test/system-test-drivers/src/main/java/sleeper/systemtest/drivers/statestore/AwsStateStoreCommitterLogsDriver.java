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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.QueryStatus;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterLogs;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterLogsDriver;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_LOG_GROUP;

public class AwsStateStoreCommitterLogsDriver implements StateStoreCommitterLogsDriver {
    public static final Logger LOGGER = LoggerFactory.getLogger(AwsStateStoreCommitterDriver.class);

    private final SystemTestInstanceContext instance;
    private final CloudWatchLogsClient cloudWatch;

    public AwsStateStoreCommitterLogsDriver(SystemTestInstanceContext instance, CloudWatchLogsClient cloudWatch) {
        this.instance = instance;
        this.cloudWatch = cloudWatch;
    }

    @Override
    public StateStoreCommitterLogs getLogsInPeriod(Instant startTime, Instant endTime) {
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
        return StateStoreCommitterLogs.from(
                waitForQuery(queryId).results().stream()
                        .map(ReadStateStoreCommitterLogs::read)
                        .collect(toUnmodifiableList()));
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