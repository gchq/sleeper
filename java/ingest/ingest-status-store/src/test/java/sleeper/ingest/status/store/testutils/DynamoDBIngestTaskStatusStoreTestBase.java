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
package sleeper.ingest.status.store.testutils;

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.tracker.ingest.task.IngestTaskFinishedStatus;
import sleeper.core.tracker.ingest.task.IngestTaskStatus;
import sleeper.core.tracker.ingest.task.IngestTaskStatusStore;
import sleeper.dynamodb.test.DynamoDBTestBase;
import sleeper.ingest.status.store.task.DynamoDBIngestTaskStatusStore;
import sleeper.ingest.status.store.task.DynamoDBIngestTaskStatusStoreCreator;
import sleeper.ingest.status.store.task.IngestTaskStatusStoreFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.UUID;

import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.IngestProperty.INGEST_TASK_STATUS_TTL_IN_SECONDS;
import static sleeper.ingest.status.store.task.DynamoDBIngestTaskStatusStore.taskStatusTableName;

public class DynamoDBIngestTaskStatusStoreTestBase extends DynamoDBTestBase {

    protected static final RecursiveComparisonConfiguration IGNORE_EXPIRY_DATE = RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("expiryDate")
            // For some reason, default Double comparator compares NaNs as object references instead of their double values
            .withComparatorForFields(Comparator.naturalOrder(),
                    "finishedStatus.recordsReadPerSecond", "finishedStatus.recordsWrittenPerSecond")
            .build();
    private final InstanceProperties instanceProperties = IngestTrackerTestUtils.createInstanceProperties();
    private final String taskStatusTableName = taskStatusTableName(instanceProperties.get(ID));
    protected final IngestTaskStatusStore store = IngestTaskStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);

    @BeforeEach
    public void setUp() {
        DynamoDBIngestTaskStatusStoreCreator.create(instanceProperties, dynamoDBClient);
    }

    @AfterEach
    public void tearDown() {
        dynamoDBClient.deleteTable(taskStatusTableName);
    }

    protected IngestTaskStatusStore storeWithTimeToLiveAndUpdateTimes(Duration timeToLive, Instant... updateTimes) {
        instanceProperties.set(INGEST_TASK_STATUS_TTL_IN_SECONDS, "" + timeToLive.getSeconds());
        return new DynamoDBIngestTaskStatusStore(dynamoDBClient, instanceProperties,
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

    private static RecordsProcessedSummary defaultJobSummary() {
        return new RecordsProcessedSummary(
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
                        .addJobSummary(new RecordsProcessedSummary(
                                new RecordsProcessed(200, 100),
                                startTime, finishTime)))
                .build();
    }

}
