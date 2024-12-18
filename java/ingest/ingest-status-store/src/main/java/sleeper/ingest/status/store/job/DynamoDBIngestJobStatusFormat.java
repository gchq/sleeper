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

package sleeper.ingest.status.store.job;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFailedStatus;
import sleeper.core.record.process.status.ProcessStatusUpdate;
import sleeper.core.record.process.status.ProcessStatusUpdateRecord;
import sleeper.core.tracker.ingest.job.update.IngestJobAddedFilesEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobFailedEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobFinishedEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobStartedEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobValidatedEvent;
import sleeper.dynamodb.tools.DynamoDBAttributes;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;
import sleeper.ingest.core.job.status.IngestJobAcceptedStatus;
import sleeper.ingest.core.job.status.IngestJobAddedFilesStatus;
import sleeper.ingest.core.job.status.IngestJobFinishedStatus;
import sleeper.ingest.core.job.status.IngestJobRejectedStatus;
import sleeper.ingest.core.job.status.IngestJobStartedStatus;
import sleeper.ingest.core.job.status.IngestJobStatus;

import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.dynamodb.tools.DynamoDBAttributes.getBooleanAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getInstantAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getIntAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getLongAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getNullableIntAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getStringListAttribute;

class DynamoDBIngestJobStatusFormat {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBIngestJobStatusFormat.class);

    static final String TABLE_ID = "TableId";
    static final String JOB_ID = "JobId";
    static final String JOB_ID_AND_UPDATE = "JobIdAndUpdate";
    static final String UPDATE_TIME = "UpdateTime";
    static final String UPDATE_TYPE = "UpdateType";
    static final String VALIDATION_TIME = "ValidationTime";
    static final String VALIDATION_RESULT = "ValidationResult";
    static final String VALIDATION_REASONS = "ValidationReasons";
    static final String JSON_MESSAGE = "JsonMessage";
    static final String INPUT_FILES_COUNT = "InputFilesCount";
    static final String START_OF_RUN = "StartOfRun";
    static final String START_TIME = "StartTime";
    static final String FILES_WRITTEN_TIME = "FilesWrittenTime";
    static final String FILES_WRITTEN_COUNT = "FilesWrittenCount";
    static final String FINISH_TIME = "FinishTime";
    static final String JOB_COMMITTED_WHEN_FILES_ADDED = "JobCommittedWhenFilesAdded";
    static final String MILLIS_IN_PROCESS = "MillisInProcess";
    static final String RECORDS_READ = "RecordsRead";
    static final String RECORDS_WRITTEN = "RecordsWritten";
    static final String FAILURE_REASONS = "FailureReasons";
    static final String JOB_RUN_ID = "JobRunId";
    static final String TASK_ID = "TaskId";
    static final String EXPIRY_DATE = "ExpiryDate";
    static final String UPDATE_TYPE_VALIDATED = "validated";
    static final String UPDATE_TYPE_STARTED = "started";
    static final String UPDATE_TYPE_ADDED_FILES = "addedFiles";
    static final String UPDATE_TYPE_FINISHED = "finished";
    static final String UPDATE_TYPE_FAILED = "failed";
    static final String VALIDATION_ACCEPTED_VALUE = "ACCEPTED";
    static final String VALIDATION_REJECTED_VALUE = "REJECTED";
    static final String TABLE_ID_UNKNOWN = "-";

    private static final Random JOB_UPDATE_ID_GENERATOR = new SecureRandom();

    private DynamoDBIngestJobStatusFormat() {
    }

    public static Map<String, AttributeValue> createJobValidatedUpdate(
            IngestJobValidatedEvent event, DynamoDBRecordBuilder builder) {
        return builder
                .string(UPDATE_TYPE, UPDATE_TYPE_VALIDATED)
                .number(VALIDATION_TIME, event.getValidationTime().toEpochMilli())
                .string(VALIDATION_RESULT, getValidationResult(event))
                .list(VALIDATION_REASONS, event.getReasons().stream()
                        .map(DynamoDBAttributes::createStringAttribute)
                        .collect(Collectors.toList()))
                .string(JSON_MESSAGE, event.getJsonMessage())
                .number(INPUT_FILES_COUNT, event.getFileCount())
                .string(JOB_RUN_ID, event.getJobRunId())
                .string(TASK_ID, event.getTaskId())
                .build();
    }

    public static String getValidationResult(IngestJobValidatedEvent event) {
        return event.isAccepted() ? VALIDATION_ACCEPTED_VALUE : VALIDATION_REJECTED_VALUE;
    }

    public static Map<String, AttributeValue> createJobStartedUpdate(
            IngestJobStartedEvent event, DynamoDBRecordBuilder builder) {
        return builder
                .string(UPDATE_TYPE, UPDATE_TYPE_STARTED)
                .number(START_TIME, event.getStartTime().toEpochMilli())
                .string(JOB_RUN_ID, event.getJobRunId())
                .string(TASK_ID, event.getTaskId())
                .number(INPUT_FILES_COUNT, event.getFileCount())
                .bool(START_OF_RUN, event.isStartOfRun())
                .build();
    }

    public static Map<String, AttributeValue> createJobAddedFilesUpdate(
            IngestJobAddedFilesEvent event, DynamoDBRecordBuilder builder) {
        return builder
                .string(UPDATE_TYPE, UPDATE_TYPE_ADDED_FILES)
                .number(FILES_WRITTEN_TIME, event.getWrittenTime().toEpochMilli())
                .string(JOB_RUN_ID, event.getJobRunId())
                .string(TASK_ID, event.getTaskId())
                .number(FILES_WRITTEN_COUNT, event.getFileCount())
                .build();
    }

    public static Map<String, AttributeValue> createJobFinishedUpdate(
            IngestJobFinishedEvent event, DynamoDBRecordBuilder builder) {
        RecordsProcessedSummary summary = event.getSummary();
        return builder
                .string(UPDATE_TYPE, UPDATE_TYPE_FINISHED)
                .number(START_TIME, summary.getStartTime().toEpochMilli())
                .string(JOB_RUN_ID, event.getJobRunId())
                .string(TASK_ID, event.getTaskId())
                .number(FINISH_TIME, summary.getFinishTime().toEpochMilli())
                .number(MILLIS_IN_PROCESS, summary.getTimeInProcess().toMillis())
                .number(RECORDS_READ, summary.getRecordsRead())
                .number(RECORDS_WRITTEN, summary.getRecordsWritten())
                .bool(JOB_COMMITTED_WHEN_FILES_ADDED, event.isCommittedBySeparateFileUpdates())
                .number(FILES_WRITTEN_COUNT, event.getNumFilesWrittenByJob())
                .build();
    }

    public static Map<String, AttributeValue> createJobFailedUpdate(
            IngestJobFailedEvent event, DynamoDBRecordBuilder builder) {
        ProcessRunTime runTime = event.getRunTime();
        return builder
                .string(UPDATE_TYPE, UPDATE_TYPE_FAILED)
                .number(START_TIME, runTime.getStartTime().toEpochMilli())
                .string(JOB_RUN_ID, event.getJobRunId())
                .string(TASK_ID, event.getTaskId())
                .number(FINISH_TIME, runTime.getFinishTime().toEpochMilli())
                .number(MILLIS_IN_PROCESS, runTime.getTimeInProcess().toMillis())
                .list(FAILURE_REASONS, event.getFailureReasons().stream()
                        .map(DynamoDBAttributes::createStringAttribute)
                        .collect(Collectors.toList()))
                .build();
    }

    public static DynamoDBRecordBuilder jobUpdateBuilder(String tableId, String jobId, Instant timeNow, Instant expiry) {
        if (tableId == null) {
            tableId = TABLE_ID_UNKNOWN;
        }
        return new DynamoDBRecordBuilder()
                .string(TABLE_ID, tableId)
                .string(JOB_ID, jobId)
                .string(JOB_ID_AND_UPDATE, jobId + "|" + timeNow.toEpochMilli() + "|" + generateJobUpdateId())
                .number(UPDATE_TIME, timeNow.toEpochMilli())
                .number(EXPIRY_DATE, expiry.getEpochSecond());
    }

    private static String generateJobUpdateId() {
        byte[] bytes = new byte[4];
        JOB_UPDATE_ID_GENERATOR.nextBytes(bytes);
        return Hex.encodeHexString(bytes);
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
                boolean accepted = !Objects.equals(VALIDATION_REJECTED_VALUE, getStringAttribute(item, VALIDATION_RESULT));
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
            case UPDATE_TYPE_ADDED_FILES:
                return IngestJobAddedFilesStatus.builder()
                        .fileCount(getIntAttribute(item, FILES_WRITTEN_COUNT, 0))
                        .writtenTime(getInstantAttribute(item, FILES_WRITTEN_TIME))
                        .updateTime(getInstantAttribute(item, UPDATE_TIME))
                        .build();
            case UPDATE_TYPE_FINISHED:
                return IngestJobFinishedStatus.updateTimeAndSummary(
                        getInstantAttribute(item, UPDATE_TIME),
                        new RecordsProcessedSummary(new RecordsProcessed(
                                getLongAttribute(item, RECORDS_READ, 0),
                                getLongAttribute(item, RECORDS_WRITTEN, 0)),
                                getRunTime(item)))
                        .committedBySeparateFileUpdates(getBooleanAttribute(item, JOB_COMMITTED_WHEN_FILES_ADDED))
                        .numFilesWrittenByJob(getNullableIntAttribute(item, FILES_WRITTEN_COUNT))
                        .build();
            case UPDATE_TYPE_FAILED:
                return ProcessFailedStatus.timeAndReasons(
                        getInstantAttribute(item, UPDATE_TIME),
                        getRunTime(item),
                        getStringListAttribute(item, FAILURE_REASONS));
            default:
                LOGGER.warn("Found record with unrecognised update type: {}", item);
                throw new IllegalArgumentException("Found record with unrecognised update type");
        }
    }

    private static ProcessRunTime getRunTime(Map<String, AttributeValue> item) {
        Instant startTime = getInstantAttribute(item, START_TIME);
        Instant finishTime = getInstantAttribute(item, FINISH_TIME);
        long millisInProcess = getLongAttribute(item, MILLIS_IN_PROCESS, -1);
        Duration timeInProcess = millisInProcess > -1
                ? Duration.ofMillis(millisInProcess)
                : Duration.between(startTime, finishTime);
        return new ProcessRunTime(startTime, finishTime, timeInProcess);
    }
}
