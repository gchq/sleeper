/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.clients.report.statestore;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.QueryStatus;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.util.ClientsGsonConfig;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.PollWithRetries;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_LOG_GROUP;

/**
 * Scans and parses the state store committer logs. Used to create statistics on the rate of transactions committed.
 */
public class QueryStateStoreCommitterLogs {
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryStateStoreCommitterLogs.class);

    private final InstanceProperties instanceProperties;
    private final CloudWatchLogsClient cloudWatch;

    public QueryStateStoreCommitterLogs(InstanceProperties instanceProperties, CloudWatchLogsClient cloudWatch) {
        this.instanceProperties = instanceProperties;
        this.cloudWatch = cloudWatch;
    }

    /**
     * Retrieves all logs in a given time period. Pages through results from CloudWatch if necessary. If this is a
     * recent period, this may involve waiting for the logs to settle until they are reliably available for query in
     * CloudWatch.
     *
     * @param  startTime            the start time
     * @param  endTime              the end time
     * @return                      the log entries in the period
     * @throws InterruptedException if the thread is interrupted while waiting for the logs to settle
     */
    public List<StateStoreCommitterLogEntry> getLogsInPeriod(Instant startTime, Instant endTime) throws InterruptedException {
        String logGroupName = instanceProperties.get(STATESTORE_COMMITTER_LOG_GROUP);
        LOGGER.info("Submitting logs query for log group {} starting at time {}", logGroupName, startTime);
        return PageThroughLogs.from(this::getSinglePageInPeriodWithLimit)
                .getLogsInPeriod(startTime, endTime);
    }

    private List<StateStoreCommitterLogEntry> getSinglePageInPeriodWithLimit(Instant startTime, Instant endTime, int limit) {
        String logGroupName = instanceProperties.get(STATESTORE_COMMITTER_LOG_GROUP);
        String queryId = cloudWatch.startQuery(builder -> builder
                .logGroupName(logGroupName)
                .startTime(startTime.getEpochSecond())
                .endTime(endTime.getEpochSecond())
                .limit(limit)
                .queryString("fields @timestamp, @message, @logStream " +
                        "| filter @message like /Lambda (started|finished) at|Applied request to table/ " +
                        "| sort @timestamp asc"))
                .queryId();
        return waitForQuery(queryId)
                .results().stream()
                .map(ReadStateStoreCommitterLogs::read)
                .collect(toUnmodifiableList());
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

    public static void main(String[] args) throws IOException, InterruptedException {
        Gson gson = ClientsGsonConfig.standardBuilder().create();
        Path inputFile = Path.of("input.json");
        LOGGER.info("Input file: {}", inputFile.toAbsolutePath());
        Input input = gson.fromJson(Files.readString(inputFile), Input.class);
        LOGGER.info("{}", input);

        try (S3Client s3 = S3Client.create();
                CloudWatchLogsClient cw = CloudWatchLogsClient.create()) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3, input.instanceId);
            QueryStateStoreCommitterLogs queryLogs = new QueryStateStoreCommitterLogs(instanceProperties, cw);
            List<StateStoreCommitterLogEntry> entries = queryLogs.getLogsInPeriod(input.startTime, input.endTime);
            LOGGER.info("Found {} entries", entries.size());
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(entries);
            LOGGER.info("Found {} runs", runs.size());
            LOGGER.info("Commits by table ID: {}", StateStoreCommitSummary.countNumCommitsByTableId(entries));
            LOGGER.info("Throughput by table ID: {}", StateStoreCommitterRequestsPerSecond.byTableIdFromRuns(runs));
        }
    }

    /**
     * The format for an input file passed on the command line, to specify the period to query in the logs.
     *
     * @param instanceId the Sleeper instance ID
     * @param startTime  the start of the period to query
     * @param endTime    the end of the period to query
     */
    public record Input(String instanceId, Instant startTime, Instant endTime) {
    }
}
