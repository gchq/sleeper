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
package sleeper.compaction.tracker.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import sleeper.core.tracker.compaction.task.CompactionTaskFinishedStatus;
import sleeper.core.tracker.compaction.task.CompactionTaskStatus;
import sleeper.core.tracker.compaction.task.CompactionTaskStatusesBuilder;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static sleeper.dynamodb.tools.DynamoDBAttributes.getDoubleAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getInstantAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getIntAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getLongAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getStringAttribute;

public class DynamoDBCompactionTaskStatusFormat {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBCompactionTaskStatusFormat.class);

    public static final String TASK_ID = "TaskId";
    public static final String UPDATE_TYPE = "UpdateType";
    public static final String START_TIME = "StartTime";
    public static final String UPDATE_TIME = "UpdateTime";
    public static final String FINISH_TIME = "FinishTime";
    public static final String MILLIS_SPENT_ON_JOBS = "MillisecondsOnJobs";
    public static final String NUMBER_OF_JOBS = "NumberOfJobs";
    public static final String ROWS_READ = "RowsRead";
    public static final String ROWS_WRITTEN = "RowsWritten";
    public static final String READ_RATE = "ReadRate";
    public static final String WRITE_RATE = "WriteRate";
    public static final String EXPIRY_DATE = "ExpiryDate";

    public static final String STARTED = "started";
    public static final String FINISHED = "finished";

    private final int timeToLiveInSeconds;
    private final Supplier<Instant> getTimeNow;

    public DynamoDBCompactionTaskStatusFormat(int timeToLiveInSeconds, Supplier<Instant> getTimeNow) {
        this.timeToLiveInSeconds = timeToLiveInSeconds;
        this.getTimeNow = getTimeNow;
    }

    public Map<String, AttributeValue> createTaskStartedRecord(CompactionTaskStatus taskStatus) {
        return createTaskRecord(taskStatus, STARTED)
                .number(START_TIME, taskStatus.getStartTime().toEpochMilli())
                .build();
    }

    public Map<String, AttributeValue> createTaskFinishedRecord(CompactionTaskStatus taskStatus) {
        return createTaskRecord(taskStatus, FINISHED)
                .number(START_TIME, taskStatus.getStartTime().toEpochMilli())
                .number(FINISH_TIME, taskStatus.getFinishedStatus().getFinishTime().toEpochMilli())
                .number(MILLIS_SPENT_ON_JOBS, taskStatus.getFinishedStatus().getTimeSpentOnJobs().toMillis())
                .number(NUMBER_OF_JOBS, taskStatus.getFinishedStatus().getTotalJobRuns())
                .number(ROWS_READ, taskStatus.getFinishedStatus().getTotalRowsRead())
                .number(ROWS_WRITTEN, taskStatus.getFinishedStatus().getTotalRowsWritten())
                .number(READ_RATE, taskStatus.getFinishedStatus().getRowsReadPerSecond())
                .number(WRITE_RATE, taskStatus.getFinishedStatus().getRowsWrittenPerSecond())
                .build();
    }

    private DynamoDBRecordBuilder createTaskRecord(CompactionTaskStatus taskStatus, String updateType) {
        Instant timeNow = getTimeNow.get();
        return new DynamoDBRecordBuilder()
                .string(TASK_ID, taskStatus.getTaskId())
                .number(UPDATE_TIME, timeNow.toEpochMilli())
                .string(UPDATE_TYPE, updateType)
                .number(EXPIRY_DATE, timeNow.getEpochSecond() + timeToLiveInSeconds);
    }

    public static Stream<CompactionTaskStatus> streamTaskStatuses(Stream<Map<String, AttributeValue>> items) {
        CompactionTaskStatusesBuilder builder = new CompactionTaskStatusesBuilder();
        items.forEach(item -> addStatusUpdate(item, builder));
        return builder.stream();
    }

    private static void addStatusUpdate(Map<String, AttributeValue> item, CompactionTaskStatusesBuilder builder) {
        String taskId = getStringAttribute(item, TASK_ID);
        switch (getStringAttribute(item, UPDATE_TYPE)) {
            case STARTED:
                builder.taskStarted(taskId,
                        getInstantAttribute(item, START_TIME),
                        getInstantAttribute(item, EXPIRY_DATE, Instant::ofEpochSecond));
                break;
            case FINISHED:
                builder.taskFinished(taskId, CompactionTaskFinishedStatus.builder()
                        .finishTime(getInstantAttribute(item, FINISH_TIME))
                        .timeSpentOnJobs(Duration.ofMillis(getLongAttribute(item, MILLIS_SPENT_ON_JOBS, 0)))
                        .totalJobRuns(getIntAttribute(item, NUMBER_OF_JOBS, 0))
                        .totalRowsRead(getLongAttribute(item, ROWS_READ, 0))
                        .totalRowsWritten(getLongAttribute(item, ROWS_WRITTEN, 0))
                        .rowsReadPerSecond(getDoubleAttribute(item, READ_RATE, 0))
                        .rowsWrittenPerSecond(getDoubleAttribute(item, WRITE_RATE, 0))
                        .build());
                break;
            default:
                LOGGER.warn("Found record with unrecognised update type: {}", item);
        }
    }
}
