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
package sleeper.compaction.tracker.job;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.tracker.compaction.job.query.CompactionJobCommittedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobCreatedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobFinishedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobStartedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.compaction.job.update.CompactionJobCommittedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobCreatedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobFailedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobFinishedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobStartedEvent;
import sleeper.core.tracker.job.run.RecordsProcessed;
import sleeper.core.tracker.job.status.JobRunFailedStatus;
import sleeper.core.tracker.job.status.JobStatusUpdate;
import sleeper.core.tracker.job.status.JobStatusUpdateRecord;
import sleeper.dynamodb.tools.DynamoDBAttributes;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;

import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getInstantAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getIntAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getLongAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getStringListAttribute;

class DynamoDBCompactionJobStatusFormat {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBCompactionJobStatusFormat.class);

    static final String TABLE_ID = "TableId";
    static final String JOB_ID = "JobId";
    static final String JOB_ID_AND_UPDATE = "JobIdAndUpdate";
    static final String UPDATE_TIME = "UpdateTime";
    static final String EXPIRY_DATE = "ExpiryDate";
    private static final String UPDATE_TYPE = "UpdateType";
    private static final String PARTITION_ID = "PartitionId";
    private static final String INPUT_FILES_COUNT = "InputFilesCount";
    private static final String START_TIME = "StartTime";
    private static final String FINISH_TIME = "FinishTime";
    private static final String COMMIT_TIME = "CommitTime";
    private static final String MILLIS_IN_PROCESS = "MillisInProcess";
    private static final String RECORDS_READ = "RecordsRead";
    private static final String RECORDS_WRITTEN = "RecordsWritten";
    private static final String FAILURE_REASONS = "FailureReasons";
    private static final String JOB_RUN_ID = "JobRunId";
    private static final String TASK_ID = "TaskId";
    private static final String UPDATE_TYPE_CREATED = "created";
    private static final String UPDATE_TYPE_STARTED = "started";
    private static final String UPDATE_TYPE_FINISHED = "finished";
    private static final String UPDATE_TYPE_COMMITTED = "committed";
    private static final String UPDATE_TYPE_FAILED = "failed";

    private static final Random JOB_UPDATE_ID_GENERATOR = new SecureRandom();

    private DynamoDBCompactionJobStatusFormat() {
    }

    public static Map<String, AttributeValue> createFilesAssignedUpdate(
            AssignJobIdRequest request, DynamoDBRecordBuilder builder) {
        builder.string(UPDATE_TYPE, UPDATE_TYPE_CREATED)
                .string(PARTITION_ID, request.getPartitionId())
                .number(INPUT_FILES_COUNT, request.getFilenames().size());
        return builder.build();
    }

    public static Map<String, AttributeValue> createJobCreated(CompactionJobCreatedEvent event, DynamoDBRecordBuilder builder) {
        builder.string(UPDATE_TYPE, UPDATE_TYPE_CREATED)
                .string(PARTITION_ID, event.getPartitionId())
                .number(INPUT_FILES_COUNT, event.getInputFilesCount());
        return builder.build();
    }

    public static Map<String, AttributeValue> createJobStartedUpdate(
            CompactionJobStartedEvent event, DynamoDBRecordBuilder builder) {
        return builder
                .string(UPDATE_TYPE, UPDATE_TYPE_STARTED)
                .number(START_TIME, event.getStartTime().toEpochMilli())
                .string(TASK_ID, event.getTaskId())
                .string(JOB_RUN_ID, event.getJobRunId())
                .build();
    }

    public static Map<String, AttributeValue> createJobFinishedUpdate(
            CompactionJobFinishedEvent event, DynamoDBRecordBuilder builder) {
        RecordsProcessed recordsProcessed = event.getRecordsProcessed();
        return builder
                .string(UPDATE_TYPE, UPDATE_TYPE_FINISHED)
                .string(TASK_ID, event.getTaskId())
                .string(JOB_RUN_ID, event.getJobRunId())
                .number(FINISH_TIME, event.getFinishTime().toEpochMilli())
                .number(MILLIS_IN_PROCESS, event.getTimeInProcess().toMillis())
                .number(RECORDS_READ, recordsProcessed.getRecordsRead())
                .number(RECORDS_WRITTEN, recordsProcessed.getRecordsWritten())
                .build();
    }

    public static Map<String, AttributeValue> createJobCommittedUpdate(
            CompactionJobCommittedEvent event, DynamoDBRecordBuilder builder) {
        return builder
                .string(UPDATE_TYPE, UPDATE_TYPE_COMMITTED)
                .string(TASK_ID, event.getTaskId())
                .string(JOB_RUN_ID, event.getJobRunId())
                .number(COMMIT_TIME, event.getCommitTime().toEpochMilli())
                .build();
    }

    public static Map<String, AttributeValue> createJobFailedUpdate(
            CompactionJobFailedEvent event, DynamoDBRecordBuilder builder) {
        return builder
                .string(UPDATE_TYPE, UPDATE_TYPE_FAILED)
                .string(TASK_ID, event.getTaskId())
                .string(JOB_RUN_ID, event.getJobRunId())
                .number(FINISH_TIME, event.getFailureTime().toEpochMilli())
                .number(MILLIS_IN_PROCESS, event.getTimeInProcess().toMillis())
                .list(FAILURE_REASONS, event.getFailureReasons().stream()
                        .map(DynamoDBAttributes::createStringAttribute)
                        .collect(toUnmodifiableList()))
                .build();
    }

    public static DynamoDBRecordBuilder jobUpdateBuilder(String tableId, String jobId, Instant timeNow, Instant expiry) {
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

    static Stream<CompactionJobStatus> streamJobStatuses(Stream<Map<String, AttributeValue>> items) {
        return CompactionJobStatus.streamFrom(items
                .map(DynamoDBCompactionJobStatusFormat::getStatusUpdateRecord));
    }

    private static JobStatusUpdateRecord getStatusUpdateRecord(Map<String, AttributeValue> item) {
        return JobStatusUpdateRecord.builder()
                .jobId(getStringAttribute(item, JOB_ID))
                .statusUpdate(getStatusUpdate(item))
                .taskId(getStringAttribute(item, TASK_ID))
                .jobRunId(getStringAttribute(item, JOB_RUN_ID))
                .expiryDate(getInstantAttribute(item, EXPIRY_DATE, Instant::ofEpochSecond))
                .build();
    }

    private static JobStatusUpdate getStatusUpdate(Map<String, AttributeValue> item) {
        switch (getStringAttribute(item, UPDATE_TYPE)) {
            case UPDATE_TYPE_CREATED:
                return CompactionJobCreatedStatus.builder()
                        .updateTime(getInstantAttribute(item, UPDATE_TIME))
                        .partitionId(getStringAttribute(item, PARTITION_ID))
                        .inputFilesCount(getIntAttribute(item, INPUT_FILES_COUNT, 0))
                        .build();
            case UPDATE_TYPE_STARTED:
                return CompactionJobStartedStatus.startAndUpdateTime(
                        getInstantAttribute(item, START_TIME),
                        getInstantAttribute(item, UPDATE_TIME));
            case UPDATE_TYPE_FINISHED:
                return CompactionJobFinishedStatus.builder()
                        .updateTime(getInstantAttribute(item, UPDATE_TIME))
                        .finishTime(getInstantAttribute(item, FINISH_TIME))
                        .timeInProcess(getTimeInProcess(item))
                        .recordsProcessed(new RecordsProcessed(
                                getLongAttribute(item, RECORDS_READ, 0),
                                getLongAttribute(item, RECORDS_WRITTEN, 0)))
                        .build();
            case UPDATE_TYPE_COMMITTED:
                return CompactionJobCommittedStatus.commitAndUpdateTime(
                        getInstantAttribute(item, COMMIT_TIME),
                        getInstantAttribute(item, UPDATE_TIME));
            case UPDATE_TYPE_FAILED:
                return JobRunFailedStatus.builder()
                        .updateTime(getInstantAttribute(item, UPDATE_TIME))
                        .failureTime(getInstantAttribute(item, FINISH_TIME))
                        .timeInProcess(getTimeInProcess(item))
                        .failureReasons(getStringListAttribute(item, FAILURE_REASONS))
                        .build();
            default:
                LOGGER.warn("Found record with unrecognised update type: {}", item);
                throw new IllegalArgumentException("Found record with unrecognised update type");
        }
    }

    private static Duration getTimeInProcess(Map<String, AttributeValue> item) {
        long millisInProcess = getLongAttribute(item, MILLIS_IN_PROCESS, -1);
        return millisInProcess > -1
                ? Duration.ofMillis(millisInProcess)
                : null;
    }
}
