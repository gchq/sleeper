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
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.status.IngestJobAcceptedStatus;
import sleeper.ingest.job.status.IngestJobFinishedEvent;
import sleeper.ingest.job.status.IngestJobRejectedStatus;
import sleeper.ingest.job.status.IngestJobStartedEvent;
import sleeper.ingest.job.status.IngestJobStartedStatus;
import sleeper.ingest.job.status.IngestJobStatus;
import sleeper.ingest.job.status.IngestJobValidatedEvent;

import java.time.Instant;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static sleeper.dynamodb.tools.DynamoDBAttributes.getBooleanAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getInstantAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getIntAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getLongAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getStringAttribute;

public class DynamoDBIngestJobStatusFormat {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBIngestJobStatusFormat.class);

    public static final String JOB_ID = "JobId";
    public static final String UPDATE_TIME = "UpdateTime";
    public static final String UPDATE_TYPE = "UpdateType";
    public static final String VALIDATION_TIME = "ValidationTime";
    public static final String VALIDATION_RESULT = "Result";
    public static final String VALIDATION_REASON = "ValidationReason";

    public static final String TABLE_NAME = "TableName";
    public static final String INPUT_FILES_COUNT = "InputFilesCount";
    public static final String START_OF_RUN = "StartOfRun";
    public static final String START_TIME = "StartTime";
    public static final String FINISH_TIME = "FinishTime";
    public static final String RECORDS_READ = "RecordsRead";
    public static final String RECORDS_WRITTEN = "RecordsWritten";
    public static final String TASK_ID = "TaskId";
    public static final String EXPIRY_DATE = "ExpiryDate";
    public static final String UPDATE_TYPE_VALIDATED = "validated";
    public static final String UPDATE_TYPE_STARTED = "started";
    public static final String UPDATE_TYPE_FINISHED = "finished";

    private final int timeToLiveInSeconds;
    private final Supplier<Instant> getTimeNow;

    public DynamoDBIngestJobStatusFormat(int timeToLiveInSeconds, Supplier<Instant> getTimeNow) {
        this.timeToLiveInSeconds = timeToLiveInSeconds;
        this.getTimeNow = getTimeNow;
    }

    public Map<String, AttributeValue> createJobValidatedRecord(IngestJobValidatedEvent event) {
        return createJobRecord(event.getJob(), UPDATE_TYPE_VALIDATED)
                .number(VALIDATION_TIME, event.getValidationTime().toEpochMilli())
                .bool(VALIDATION_RESULT, event.isAccepted())
                .string(VALIDATION_REASON, event.getReason())
                .string(TASK_ID, event.getTaskId())
                .build();
    }

    public Map<String, AttributeValue> createJobStartedRecord(IngestJobStartedEvent event) {
        return createJobRecord(event.getJob(), UPDATE_TYPE_STARTED)
                .number(START_TIME, event.getStartTime().toEpochMilli())
                .string(TASK_ID, event.getTaskId())
                .number(INPUT_FILES_COUNT, event.getJob().getFiles().size())
                .bool(START_OF_RUN, event.isStartOfRun())
                .build();
    }

    public Map<String, AttributeValue> createJobFinishedRecord(IngestJobFinishedEvent event) {
        RecordsProcessedSummary summary = event.getSummary();
        return createJobRecord(event.getJob(), UPDATE_TYPE_FINISHED)
                .number(START_TIME, summary.getStartTime().toEpochMilli())
                .string(TASK_ID, event.getTaskId())
                .number(FINISH_TIME, summary.getFinishTime().toEpochMilli())
                .number(RECORDS_READ, summary.getRecordsRead())
                .number(RECORDS_WRITTEN, summary.getRecordsWritten())
                .build();
    }

    private DynamoDBRecordBuilder createJobRecord(IngestJob job, String updateType) {
        Instant timeNow = getTimeNow.get();
        return new DynamoDBRecordBuilder()
                .string(JOB_ID, job.getId())
                .string(TABLE_NAME, job.getTableName())
                .number(UPDATE_TIME, timeNow.toEpochMilli())
                .string(UPDATE_TYPE, updateType)
                .number(EXPIRY_DATE, timeNow.getEpochSecond() + timeToLiveInSeconds);
    }

    public static Stream<IngestJobStatus> streamJobStatuses(Stream<Map<String, AttributeValue>> items) {
        return IngestJobStatus.streamFrom(items
                .map(DynamoDBIngestJobStatusFormat::getStatusUpdateRecord));
    }

    private static ProcessStatusUpdateRecord getStatusUpdateRecord(Map<String, AttributeValue> item) {
        return new ProcessStatusUpdateRecord(
                getStringAttribute(item, JOB_ID),
                getInstantAttribute(item, EXPIRY_DATE, Instant::ofEpochSecond),
                getStatusUpdate(item),
                getStringAttribute(item, TASK_ID));
    }

    private static ProcessStatusUpdate getStatusUpdate(Map<String, AttributeValue> item) {
        switch (getStringAttribute(item, UPDATE_TYPE)) {
            case UPDATE_TYPE_VALIDATED:
                boolean accepted = getBooleanAttribute(item, VALIDATION_RESULT);
                if (accepted) {
                    return IngestJobAcceptedStatus
                            .validationTime(getInstantAttribute(item, VALIDATION_TIME));
                } else {
                    return IngestJobRejectedStatus.builder()
                            .validationTime(getInstantAttribute(item, VALIDATION_TIME))
                            .reason(getStringAttribute(item, VALIDATION_REASON))
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
