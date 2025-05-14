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

package sleeper.ingest.trackerv2.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import sleeper.core.tracker.ingest.task.IngestTaskFinishedStatus;
import sleeper.core.tracker.ingest.task.IngestTaskStatus;
import sleeper.core.tracker.ingest.task.IngestTaskStatusesBuilder;
import sleeper.dynamodb.toolsv2.DynamoDBRecordBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static sleeper.dynamodb.toolsv2.DynamoDBAttributes.getDoubleAttribute;
import static sleeper.dynamodb.toolsv2.DynamoDBAttributes.getInstantAttribute;
import static sleeper.dynamodb.toolsv2.DynamoDBAttributes.getIntAttribute;
import static sleeper.dynamodb.toolsv2.DynamoDBAttributes.getLongAttribute;
import static sleeper.dynamodb.toolsv2.DynamoDBAttributes.getStringAttribute;

public class DynamoDBIngestTaskStatusFormat {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBIngestTaskStatusFormat.class);

    public static final String TASK_ID = "TaskId";
    public static final String UPDATE_TYPE = "UpdateType";
    public static final String START_TIME = "StartTime";
    public static final String UPDATE_TIME = "UpdateTime";
    public static final String FINISH_TIME = "FinishTime";
    public static final String MILLIS_SPENT_ON_JOBS = "MillisecondsOnJobs";
    public static final String NUMBER_OF_JOBS = "NumberOfJobs";
    public static final String RECORDS_READ = "RecordsRead";
    public static final String RECORDS_WRITTEN = "RecordsWritten";
    public static final String READ_RATE = "ReadRate";
    public static final String WRITE_RATE = "WriteRate";
    public static final String EXPIRY_DATE = "ExpiryDate";

    public static final String STARTED = "started";
    public static final String FINISHED = "finished";

    private final int timeToLiveInSeconds;
    private final Supplier<Instant> getTimeNow;

    public DynamoDBIngestTaskStatusFormat(int timeToLiveInSeconds, Supplier<Instant> getTimeNow) {
        this.timeToLiveInSeconds = timeToLiveInSeconds;
        this.getTimeNow = getTimeNow;
    }

    public Map<String, AttributeValue> createTaskStartedRecord(IngestTaskStatus taskStatus) {
        return createTaskRecord(taskStatus, STARTED)
                .number(START_TIME, taskStatus.getStartTime().toEpochMilli())
                .build();
    }

    public Map<String, AttributeValue> createTaskFinishedRecord(IngestTaskStatus taskStatus) {
        return createTaskRecord(taskStatus, FINISHED)
                .number(START_TIME, taskStatus.getStartTime().toEpochMilli())
                .number(FINISH_TIME, taskStatus.getFinishedStatus().getFinishTime().toEpochMilli())
                .number(MILLIS_SPENT_ON_JOBS, taskStatus.getFinishedStatus().getTimeSpentOnJobs().toMillis())
                .number(NUMBER_OF_JOBS, taskStatus.getFinishedStatus().getTotalJobRuns())
                .number(RECORDS_READ, taskStatus.getFinishedStatus().getTotalRecordsRead())
                .number(RECORDS_WRITTEN, taskStatus.getFinishedStatus().getTotalRecordsWritten())
                .number(READ_RATE, taskStatus.getFinishedStatus().getRecordsReadPerSecond())
                .number(WRITE_RATE, taskStatus.getFinishedStatus().getRecordsWrittenPerSecond())
                .build();
    }

    private DynamoDBRecordBuilder createTaskRecord(IngestTaskStatus taskStatus, String updateType) {
        Instant timeNow = getTimeNow.get();
        return new DynamoDBRecordBuilder()
                .string(TASK_ID, taskStatus.getTaskId())
                .number(UPDATE_TIME, timeNow.toEpochMilli())
                .string(UPDATE_TYPE, updateType)
                .number(EXPIRY_DATE, timeNow.getEpochSecond() + timeToLiveInSeconds);
    }

    public static Stream<IngestTaskStatus> streamTaskStatuses(Stream<Map<String, AttributeValue>> items) {
        IngestTaskStatusesBuilder builder = new IngestTaskStatusesBuilder();
        items.forEach(item -> addStatusUpdate(item, builder));
        return builder.stream();
    }

    private static void addStatusUpdate(Map<String, AttributeValue> item, IngestTaskStatusesBuilder builder) {
        String taskId = getStringAttribute(item, TASK_ID);
        switch (getStringAttribute(item, UPDATE_TYPE)) {
            case STARTED:
                builder.taskStarted(taskId,
                        getInstantAttribute(item, START_TIME),
                        getInstantAttribute(item, EXPIRY_DATE, Instant::ofEpochSecond));
                break;
            case FINISHED:
                builder.taskFinished(taskId, IngestTaskFinishedStatus.builder()
                        .finishTime(getInstantAttribute(item, FINISH_TIME))
                        .timeSpentOnJobs(Duration.ofMillis(getLongAttribute(item, MILLIS_SPENT_ON_JOBS, 0)))
                        .totalJobRuns(getIntAttribute(item, NUMBER_OF_JOBS, 0))
                        .totalRecordsRead(getLongAttribute(item, RECORDS_READ, 0))
                        .totalRecordsWritten(getLongAttribute(item, RECORDS_WRITTEN, 0))
                        .recordsReadPerSecond(getDoubleAttribute(item, READ_RATE, 0))
                        .recordsWrittenPerSecond(getDoubleAttribute(item, WRITE_RATE, 0))
                        .build());
                break;
            default:
                LOGGER.warn("Found record with unrecognised update type: {}", item);
        }
    }
}
