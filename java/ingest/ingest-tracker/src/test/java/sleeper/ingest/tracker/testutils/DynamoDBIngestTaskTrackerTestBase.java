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
package sleeper.ingest.tracker.testutils;

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.BeforeEach;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.tracker.ingest.task.IngestTaskFinishedStatus;
import sleeper.core.tracker.ingest.task.IngestTaskStatus;
import sleeper.core.tracker.ingest.task.IngestTaskTracker;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.RecordsProcessed;
import sleeper.ingest.tracker.task.DynamoDBIngestTaskTracker;
import sleeper.ingest.tracker.task.DynamoDBIngestTaskTrackerCreator;
import sleeper.ingest.tracker.task.IngestTaskTrackerFactory;
import sleeper.localstack.test.LocalStackTestBase;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.UUID;

import static sleeper.core.properties.instance.IngestProperty.INGEST_TASK_STATUS_TTL_IN_SECONDS;

public class DynamoDBIngestTaskTrackerTestBase extends LocalStackTestBase {

    protected static final RecursiveComparisonConfiguration IGNORE_EXPIRY_DATE = RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("expiryDate")
            // For some reason, default Double comparator compares NaNs as object references instead of their double values
            .withComparatorForFields(Comparator.naturalOrder(),
                    "finishedStatus.recordsReadPerSecond", "finishedStatus.recordsWrittenPerSecond")
            .build();
    private final InstanceProperties instanceProperties = IngestTrackerTestUtils.createInstanceProperties();
    protected final IngestTaskTracker tracker = IngestTaskTrackerFactory.getTracker(dynamoClient, instanceProperties);

    @BeforeEach
    public void setUp() {
        DynamoDBIngestTaskTrackerCreator.create(instanceProperties, dynamoClient);
    }

    protected IngestTaskTracker trackerWithTimeToLiveAndUpdateTimes(Duration timeToLive, Instant... updateTimes) {
        instanceProperties.set(INGEST_TASK_STATUS_TTL_IN_SECONDS, "" + timeToLive.getSeconds());
        return new DynamoDBIngestTaskTracker(dynamoClient, instanceProperties,
                Arrays.stream(updateTimes).iterator()::next);
    }

    private static Instant defaultJobStartTime() {
        return Instant.parse("2022-09-22T14:00:04.000Z");
    }

    private static Instant defaultJobFinishTime() {
        return Instant.parse("2022-09-22T14:00:14.000Z");
    }

    protected static Instant defaultTaskStartTime() {
        return Instant.parse("2022-09-22T12:30:00.000Z");
    }

    protected static Instant defaultTaskFinishTime() {
        return Instant.parse("2022-09-22T16:30:00.000Z");
    }

    private static Instant taskFinishTimeWithDurationInSecondsNotAWholeNumber() {
        return Instant.parse("2022-09-22T16:30:00.500Z");
    }

    private static JobRunSummary defaultJobSummary() {
        return new JobRunSummary(
                new RecordsProcessed(4800L, 2400L),
                defaultJobStartTime(), defaultJobFinishTime());
    }

    protected static IngestTaskStatus startedTaskWithDefaults() {
        return startedTaskWithDefaultsBuilder().build();
    }

    protected static IngestTaskStatus.Builder startedTaskWithDefaultsBuilder() {
        return IngestTaskStatus.builder().taskId(UUID.randomUUID().toString()).startTime(defaultTaskStartTime());
    }

    protected static IngestTaskStatus finishedTaskWithDefaults() {
        return startedTaskWithDefaultsBuilder().finished(
                defaultTaskFinishTime(), IngestTaskFinishedStatus.builder()
                        .addJobSummary(defaultJobSummary()))
                .build();
    }

    protected static IngestTaskStatus finishedTaskWithDefaultsAndDurationInSecondsNotAWholeNumber() {
        return startedTaskWithDefaultsBuilder().finished(
                taskFinishTimeWithDurationInSecondsNotAWholeNumber(), IngestTaskFinishedStatus.builder()
                        .addJobSummary(defaultJobSummary()))
                .build();
    }

    protected static IngestTaskStatus finishedTaskWithNoJobsAndZeroDuration() {
        return startedTaskWithDefaultsBuilder().finished(
                defaultTaskStartTime(), IngestTaskFinishedStatus.builder()).build();
    }

    protected static IngestTaskStatus taskWithStartTime(Instant startTime) {
        return taskBuilder().startTime(startTime).build();
    }

    protected static IngestTaskStatus taskWithStartAndFinishTime(Instant startTime, Instant finishTime) {
        return buildWithStartAndFinishTime(taskBuilder(), startTime, finishTime);
    }

    private static IngestTaskStatus.Builder taskBuilder() {
        return IngestTaskStatus.builder().taskId(UUID.randomUUID().toString());
    }

    private static IngestTaskStatus buildWithStartAndFinishTime(
            IngestTaskStatus.Builder builder, Instant startTime, Instant finishTime) {
        return builder.startTime(startTime)
                .finished(finishTime, IngestTaskFinishedStatus.builder()
                        .addJobSummary(new JobRunSummary(
                                new RecordsProcessed(200, 100),
                                startTime, finishTime)))
                .build();
    }

}
