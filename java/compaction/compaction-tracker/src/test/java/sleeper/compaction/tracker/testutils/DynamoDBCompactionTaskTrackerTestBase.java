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
package sleeper.compaction.tracker.testutils;

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import sleeper.compaction.tracker.task.CompactionTaskTrackerFactory;
import sleeper.compaction.tracker.task.DynamoDBCompactionTaskTracker;
import sleeper.compaction.tracker.task.DynamoDBCompactionTaskTrackerCreator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.tracker.compaction.task.CompactionTaskFinishedStatus;
import sleeper.core.tracker.compaction.task.CompactionTaskStatus;
import sleeper.core.tracker.compaction.task.CompactionTaskTracker;
import sleeper.core.tracker.job.RecordsProcessed;
import sleeper.core.tracker.job.RecordsProcessedSummary;
import sleeper.dynamodb.test.DynamoDBTestBase;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.UUID;

import static sleeper.compaction.tracker.task.DynamoDBCompactionTaskTracker.taskStatusTableName;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_STATUS_TTL_IN_SECONDS;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class DynamoDBCompactionTaskTrackerTestBase extends DynamoDBTestBase {

    protected static final RecursiveComparisonConfiguration IGNORE_EXPIRY_DATE = RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("expiryDate").build();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final String taskStatusTableName = taskStatusTableName(instanceProperties.get(ID));
    protected final CompactionTaskTracker tracker = CompactionTaskTrackerFactory.getTracker(dynamoDBClient, instanceProperties);

    @BeforeEach
    public void setUp() {
        DynamoDBCompactionTaskTrackerCreator.create(instanceProperties, dynamoDBClient);
    }

    @AfterEach
    public void tearDown() {
        dynamoDBClient.deleteTable(taskStatusTableName);
    }

    protected CompactionTaskTracker trackerWithTimeToLiveAndUpdateTimes(Duration timeToLive, Instant... updateTimes) {
        instanceProperties.set(COMPACTION_TASK_STATUS_TTL_IN_SECONDS, "" + timeToLive.getSeconds());
        return new DynamoDBCompactionTaskTracker(dynamoDBClient, instanceProperties,
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

    protected static CompactionTaskStatus startedTaskWithDefaults() {
        return startedTaskWithDefaultsBuilder().build();
    }

    protected static CompactionTaskStatus.Builder startedTaskWithDefaultsBuilder() {
        return CompactionTaskStatus.builder().taskId(UUID.randomUUID().toString()).startTime(defaultTaskStartTime());
    }

    protected static CompactionTaskStatus finishedTaskWithDefaults() {
        return startedTaskWithDefaultsBuilder().finished(
                defaultTaskFinishTime(), CompactionTaskFinishedStatus.builder()
                        .addJobSummary(defaultJobSummary()))
                .build();
    }

    protected static CompactionTaskStatus finishedTaskWithDefaultsAndDurationInSecondsNotAWholeNumber() {
        return startedTaskWithDefaultsBuilder().finished(
                taskFinishTimeWithDurationInSecondsNotAWholeNumber(), CompactionTaskFinishedStatus.builder()
                        .addJobSummary(defaultJobSummary()))
                .build();
    }

    protected static CompactionTaskStatus taskWithStartTime(Instant startTime) {
        return taskBuilder().startTime(startTime).build();
    }

    protected static CompactionTaskStatus taskWithStartAndFinishTime(Instant startTime, Instant finishTime) {
        return buildWithStartAndFinishTime(taskBuilder(), startTime, finishTime);
    }

    private static CompactionTaskStatus.Builder taskBuilder() {
        return CompactionTaskStatus.builder().taskId(UUID.randomUUID().toString());
    }

    private static CompactionTaskStatus buildWithStartAndFinishTime(
            CompactionTaskStatus.Builder builder, Instant startTime, Instant finishTime) {
        return builder.startTime(startTime)
                .finished(finishTime, CompactionTaskFinishedStatus.builder()
                        .addJobSummary(new RecordsProcessedSummary(
                                new RecordsProcessed(200, 100),
                                startTime, finishTime)))
                .build();
    }

}
