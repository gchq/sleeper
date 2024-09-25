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
package sleeper.compaction.status.store.testutils;

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import sleeper.compaction.status.store.task.CompactionTaskStatusStoreFactory;
import sleeper.compaction.status.store.task.DynamoDBCompactionTaskStatusStore;
import sleeper.compaction.status.store.task.DynamoDBCompactionTaskStatusStoreCreator;
import sleeper.compaction.task.CompactionTaskFinishedStatus;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.dynamodb.test.DynamoDBTestBase;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.UUID;

import static sleeper.compaction.status.store.task.DynamoDBCompactionTaskStatusStore.taskStatusTableName;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_STATUS_TTL_IN_SECONDS;

public class DynamoDBCompactionTaskStatusStoreTestBase extends DynamoDBTestBase {

    protected static final RecursiveComparisonConfiguration IGNORE_EXPIRY_DATE = RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("expiryDate").build();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final String taskStatusTableName = taskStatusTableName(instanceProperties.get(ID));
    protected final CompactionTaskStatusStore store = CompactionTaskStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);

    @BeforeEach
    public void setUp() {
        DynamoDBCompactionTaskStatusStoreCreator.create(instanceProperties, dynamoDBClient);
    }

    @AfterEach
    public void tearDown() {
        dynamoDBClient.deleteTable(taskStatusTableName);
    }

    protected CompactionTaskStatusStore storeWithTimeToLiveAndUpdateTimes(Duration timeToLive, Instant... updateTimes) {
        instanceProperties.set(COMPACTION_TASK_STATUS_TTL_IN_SECONDS, "" + timeToLive.getSeconds());
        return new DynamoDBCompactionTaskStatusStore(dynamoDBClient, instanceProperties,
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
