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
package sleeper.compaction.status.store.job;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobStartedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessStatusUpdate;
import sleeper.core.record.process.status.ProcessStatusUpdateRecord;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static sleeper.dynamodb.tools.DynamoDBAttributes.getInstantAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getIntAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getLongAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getStringAttribute;

class DynamoDBCompactionJobStatusFormat {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBCompactionJobStatusFormat.class);

    static final String TABLE_ID = "TableId";
    static final String JOB_ID = "JobId";
    static final String UPDATE_ID = "UpdateId";
    static final String UPDATE_TIME = "UpdateTime";
    static final String EXPIRY_DATE = "ExpiryDate";
    private static final String UPDATE_TYPE = "UpdateType";
    private static final String PARTITION_ID = "PartitionId";
    private static final String INPUT_FILES_COUNT = "InputFilesCount";
    private static final String SPLIT_TO_PARTITION_IDS = "SplitToPartitionIds";
    private static final String START_TIME = "StartTime";
    private static final String FINISH_TIME = "FinishTime";
    private static final String RECORDS_READ = "RecordsRead";
    private static final String RECORDS_WRITTEN = "RecordsWritten";
    private static final String TASK_ID = "TaskId";
    private static final String UPDATE_TYPE_CREATED = "created";
    private static final String UPDATE_TYPE_STARTED = "started";
    private static final String UPDATE_TYPE_FINISHED = "finished";
    private static final SecureRandom RANDOM = new SecureRandom();

    private final int timeToLiveInSeconds;
    private final Supplier<Instant> getTimeNow;

    DynamoDBCompactionJobStatusFormat(int timeToLiveInSeconds, Supplier<Instant> getTimeNow) {
        this.timeToLiveInSeconds = timeToLiveInSeconds;
        this.getTimeNow = getTimeNow;
    }

    public Map<String, AttributeValue> createJobCreatedRecord(CompactionJob job) {
        return createJobRecord(job, UPDATE_TYPE_CREATED)
                .string(PARTITION_ID, job.getPartitionId())
                .number(INPUT_FILES_COUNT, job.getInputFiles().size())
                .apply(builder -> {
                    if (job.isSplittingJob()) {
                        builder.string(SPLIT_TO_PARTITION_IDS, String.join(", ", job.getChildPartitions()));
                    }
                }).build();
    }

    public Map<String, AttributeValue> createJobStartedRecord(CompactionJob job, Instant startTime, String taskId) {
        return createJobRecord(job, UPDATE_TYPE_STARTED)
                .number(START_TIME, startTime.toEpochMilli())
                .string(TASK_ID, taskId)
                .build();
    }

    public Map<String, AttributeValue> createJobFinishedRecord(CompactionJob job, RecordsProcessedSummary summary, String taskId) {
        return createJobRecord(job, UPDATE_TYPE_FINISHED)
                .number(START_TIME, summary.getStartTime().toEpochMilli())
                .string(TASK_ID, taskId)
                .number(FINISH_TIME, summary.getFinishTime().toEpochMilli())
                .number(RECORDS_READ, summary.getRecordsRead())
                .number(RECORDS_WRITTEN, summary.getRecordsWritten())
                .build();
    }

    private DynamoDBRecordBuilder createJobRecord(CompactionJob job, String updateType) {
        Instant timeNow = getTimeNow.get();
        ByteBuffer id = ByteBuffer.wrap(new byte[12]);
        id.putLong(timeNow.toEpochMilli());
        id.putInt(RANDOM.nextInt());
        return new DynamoDBRecordBuilder()
                .string(JOB_ID, job.getId())
                .string(TABLE_ID, job.getTableId())
                .bytes(UPDATE_ID, id.array())
                .number(UPDATE_TIME, timeNow.toEpochMilli())
                .string(UPDATE_TYPE, updateType)
                .number(EXPIRY_DATE, timeNow.getEpochSecond() + timeToLiveInSeconds);
    }

    static Stream<CompactionJobStatus> streamJobStatuses(Stream<Map<String, AttributeValue>> items) {
        return CompactionJobStatus.streamFrom(items
                .map(DynamoDBCompactionJobStatusFormat::getStatusUpdateRecord));
    }

    private static ProcessStatusUpdateRecord getStatusUpdateRecord(Map<String, AttributeValue> item) {
        return ProcessStatusUpdateRecord.builder()
                .jobId(getStringAttribute(item, JOB_ID))
                .statusUpdate(getStatusUpdate(item))
                .taskId(getStringAttribute(item, TASK_ID))
                .expiryDate(getInstantAttribute(item, EXPIRY_DATE, Instant::ofEpochSecond))
                .build();
    }

    private static ProcessStatusUpdate getStatusUpdate(Map<String, AttributeValue> item) {
        switch (getStringAttribute(item, UPDATE_TYPE)) {
            case UPDATE_TYPE_CREATED:
                return CompactionJobCreatedStatus.builder()
                        .updateTime(getInstantAttribute(item, UPDATE_TIME))
                        .partitionId(getStringAttribute(item, PARTITION_ID))
                        .childPartitionIds(getChildPartitionIds(item))
                        .inputFilesCount(getIntAttribute(item, INPUT_FILES_COUNT, 0))
                        .build();
            case UPDATE_TYPE_STARTED:
                return CompactionJobStartedStatus.startAndUpdateTime(
                        getInstantAttribute(item, START_TIME),
                        getInstantAttribute(item, UPDATE_TIME));
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

    private static List<String> getChildPartitionIds(Map<String, AttributeValue> item) {
        String string = getStringAttribute(item, SPLIT_TO_PARTITION_IDS);
        if (string == null) {
            return Collections.emptyList();
        }
        return Arrays.asList(string.split(", "));
    }
}
