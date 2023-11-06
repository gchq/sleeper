/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.ingest.status.store.job;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessStatusUpdate;
import sleeper.core.record.process.status.ProcessStatusUpdateRecord;
import sleeper.dynamodb.tools.DynamoDBAttributes;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;
import sleeper.ingest.job.status.IngestJobAcceptedStatus;
import sleeper.ingest.job.status.IngestJobFinishedEvent;
import sleeper.ingest.job.status.IngestJobRejectedStatus;
import sleeper.ingest.job.status.IngestJobStartedEvent;
import sleeper.ingest.job.status.IngestJobStartedStatus;
import sleeper.ingest.job.status.IngestJobStatus;
import sleeper.ingest.job.status.IngestJobValidatedEvent;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.dynamodb.tools.DynamoDBAttributes.getBooleanAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getInstantAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getIntAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getLongAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getStringListAttribute;

class DynamoDBIngestJobStatusFormat {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBIngestJobStatusFormat.class);

    static final String TABLE_ID = "TableId";
    static final String JOB_ID = "JobId";
    static final String JOB_ID_AND_TIME = "JobIdAndTime";
    static final String UPDATE_TIME = "UpdateTime";
    static final String UPDATE_TYPE = "UpdateType";
    static final String VALIDATION_TIME = "ValidationTime";
    static final String VALIDATION_REJECTED = "ValidationRejected";
    static final String VALIDATION_REASONS = "ValidationReasons";
    static final String JSON_MESSAGE = "JsonMessage";
    static final String TABLE_NAME = "TableName";
    static final String INPUT_FILES_COUNT = "InputFilesCount";
    static final String START_OF_RUN = "StartOfRun";
    static final String START_TIME = "StartTime";
    static final String FINISH_TIME = "FinishTime";
    static final String RECORDS_READ = "RecordsRead";
    static final String RECORDS_WRITTEN = "RecordsWritten";
    static final String JOB_RUN_ID = "JobRunId";
    static final String TASK_ID = "TaskId";
    static final String EXPIRY_DATE = "ExpiryDate";
    static final String UPDATE_TYPE_VALIDATED = "validated";
    static final String UPDATE_TYPE_STARTED = "started";
    static final String UPDATE_TYPE_FINISHED = "finished";
    static final String VALIDATION_REJECTED_VALUE = "REJECTED";
    static final String TABLE_ID_UNKNOWN = "-";

    private final int timeToLiveInSeconds;
    private final Supplier<Instant> getTimeNow;

    DynamoDBIngestJobStatusFormat(int timeToLiveInSeconds, Supplier<Instant> getTimeNow) {
        this.timeToLiveInSeconds = timeToLiveInSeconds;
        this.getTimeNow = getTimeNow;
    }

    public Map<String, AttributeValue> createJobValidatedRecord(IngestJobValidatedEvent event) {
        String tableId = Optional.ofNullable(event.getTableId()).orElse(TABLE_ID_UNKNOWN);
        return createRecord(tableId, event.getJobId(), UPDATE_TYPE_VALIDATED)
                .string(TABLE_NAME, event.getTableName())
                .number(VALIDATION_TIME, event.getValidationTime().toEpochMilli())
                .string(VALIDATION_REJECTED, event.isAccepted() ? null : VALIDATION_REJECTED_VALUE)
                .list(VALIDATION_REASONS, event.getReasons().stream()
                        .map(DynamoDBAttributes::createStringAttribute)
                        .collect(Collectors.toList()))
                .string(JSON_MESSAGE, event.getJsonMessage())
                .number(INPUT_FILES_COUNT, event.getFileCount())
                .string(JOB_RUN_ID, event.getJobRunId())
                .string(TASK_ID, event.getTaskId())
                .build();
    }

    public Map<String, AttributeValue> createJobStartedRecord(IngestJobStartedEvent event) {
        return createRecord(event.getTableId(), event.getJobId(), UPDATE_TYPE_STARTED)
                .string(TABLE_NAME, event.getTableName())
                .number(START_TIME, event.getStartTime().toEpochMilli())
                .string(JOB_RUN_ID, event.getJobRunId())
                .string(TASK_ID, event.getTaskId())
                .number(INPUT_FILES_COUNT, event.getFileCount())
                .bool(START_OF_RUN, event.isStartOfRun())
                .build();
    }

    public Map<String, AttributeValue> createJobFinishedRecord(IngestJobFinishedEvent event) {
        RecordsProcessedSummary summary = event.getSummary();
        return createRecord(event.getTableId(), event.getJobId(), UPDATE_TYPE_FINISHED)
                .string(TABLE_NAME, event.getTableName())
                .number(START_TIME, summary.getStartTime().toEpochMilli())
                .string(JOB_RUN_ID, event.getJobRunId())
                .string(TASK_ID, event.getTaskId())
                .number(FINISH_TIME, summary.getFinishTime().toEpochMilli())
                .number(RECORDS_READ, summary.getRecordsRead())
                .number(RECORDS_WRITTEN, summary.getRecordsWritten())
                .build();
    }

    private DynamoDBRecordBuilder createRecord(String tableId, String jobId, String updateType) {
        Instant timeNow = getTimeNow.get();
        return new DynamoDBRecordBuilder()
                .string(TABLE_ID, tableId)
                .string(JOB_ID, jobId)
                .number(UPDATE_TIME, timeNow.toEpochMilli())
                .string(JOB_ID_AND_TIME, jobId + "|" + timeNow.toEpochMilli())
                .string(UPDATE_TYPE, updateType)
                .number(EXPIRY_DATE, timeNow.getEpochSecond() + timeToLiveInSeconds);
    }

    public static Stream<IngestJobStatus> streamJobStatuses(Stream<Map<String, AttributeValue>> items) {
        return IngestJobStatus.streamFrom(items
                .map(DynamoDBIngestJobStatusFormat::getStatusUpdateRecord));
    }

    private static ProcessStatusUpdateRecord getStatusUpdateRecord(Map<String, AttributeValue> item) {
        return ProcessStatusUpdateRecord.builder()
                .jobId(getStringAttribute(item, JOB_ID))
                .statusUpdate(getStatusUpdate(item))
                .jobRunId(getStringAttribute(item, JOB_RUN_ID))
                .taskId(getStringAttribute(item, TASK_ID))
                .expiryDate(getInstantAttribute(item, EXPIRY_DATE, Instant::ofEpochSecond))
                .build();
    }

    private static ProcessStatusUpdate getStatusUpdate(Map<String, AttributeValue> item) {
        switch (getStringAttribute(item, UPDATE_TYPE)) {
            case UPDATE_TYPE_VALIDATED:
                boolean accepted = !Objects.equals(VALIDATION_REJECTED_VALUE, getStringAttribute(item, VALIDATION_REJECTED));
                if (accepted) {
                    return IngestJobAcceptedStatus.from(
                            getIntAttribute(item, INPUT_FILES_COUNT, 0),
                            getInstantAttribute(item, VALIDATION_TIME),
                            getInstantAttribute(item, UPDATE_TIME));
                } else {
                    return IngestJobRejectedStatus.builder()
                            .inputFileCount(getIntAttribute(item, INPUT_FILES_COUNT, 0))
                            .validationTime(getInstantAttribute(item, VALIDATION_TIME))
                            .updateTime(getInstantAttribute(item, UPDATE_TIME))
                            .reasons(getStringListAttribute(item, VALIDATION_REASONS))
                            .jsonMessage(getStringAttribute(item, JSON_MESSAGE))
                            .build();
                }
            case UPDATE_TYPE_STARTED:
                return IngestJobStartedStatus.withStartOfRun(getBooleanAttribute(item, START_OF_RUN))
                        .inputFileCount(getIntAttribute(item, INPUT_FILES_COUNT, 0))
                        .startTime(getInstantAttribute(item, START_TIME))
                        .updateTime(getInstantAttribute(item, UPDATE_TIME)).build();
            case UPDATE_TYPE_FINISHED:
                return ProcessFinishedStatus.updateTimeAndSummary(
                        getInstantAttribute(item, UPDATE_TIME),
                        new RecordsProcessedSummary(new RecordsProcessed(
                                getLongAttribute(item, RECORDS_READ, 0),
                                getLongAttribute(item, RECORDS_WRITTEN, 0)),
                                getInstantAttribute(item, START_TIME),
                                getInstantAttribute(item, FINISH_TIME)));
            default:
                LOGGER.warn("Found record with unrecognised update type: {}", item);
                throw new IllegalArgumentException("Found record with unrecognised update type");
        }
    }

}
