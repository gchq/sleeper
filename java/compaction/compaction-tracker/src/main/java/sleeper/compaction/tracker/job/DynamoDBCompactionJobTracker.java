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
package sleeper.compaction.tracker.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import sleeper.compaction.trackerv2.CompactionTrackerException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.compaction.job.update.CompactionJobCommittedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobCreatedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobFailedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobFinishedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobStartedEvent;
import sleeper.core.util.LoggedDuration;
import sleeper.dynamodb.toolsv2.DynamoDBRecordBuilder;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static sleeper.compaction.trackerv2.job.DynamoDBCompactionJobStatusFormat.UPDATE_TIME;
import static sleeper.compaction.trackerv2.job.DynamoDBCompactionJobStatusFormat.createJobCommittedUpdate;
import static sleeper.compaction.trackerv2.job.DynamoDBCompactionJobStatusFormat.createJobCreated;
import static sleeper.compaction.trackerv2.job.DynamoDBCompactionJobStatusFormat.createJobFailedUpdate;
import static sleeper.compaction.trackerv2.job.DynamoDBCompactionJobStatusFormat.createJobFinishedUpdate;
import static sleeper.compaction.trackerv2.job.DynamoDBCompactionJobStatusFormat.createJobStartedUpdate;
import static sleeper.compaction.trackerv2.task.DynamoDBCompactionTaskStatusFormat.UPDATE_TYPE;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_STATUS_TTL_IN_SECONDS;
import static sleeper.dynamodb.toolsv2.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.toolsv2.DynamoDBAttributes.getStringAttribute;
import static sleeper.dynamodb.toolsv2.DynamoDBUtils.instanceTableName;
import static sleeper.dynamodb.toolsv2.DynamoDBUtils.streamPagedItems;

public class DynamoDBCompactionJobTracker implements CompactionJobTracker {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBCompactionJobTracker.class);
    public static final String TABLE_ID = DynamoDBCompactionJobStatusFormat.TABLE_ID;
    public static final String JOB_ID = DynamoDBCompactionJobStatusFormat.JOB_ID;
    public static final String JOB_ID_AND_UPDATE = DynamoDBCompactionJobStatusFormat.JOB_ID_AND_UPDATE;
    public static final String EXPIRY_DATE = DynamoDBCompactionJobStatusFormat.EXPIRY_DATE;
    private static final String JOB_FIRST_UPDATE_TIME = "FirstUpdateTime";
    private static final String JOB_LAST_UPDATE_TIME = "LastUpdateTime";
    private static final String JOB_LAST_UPDATE_TYPE = "LastUpdateType";

    private final DynamoDbClient dynamoDB;
    private final String updatesTableName;
    private final String jobsTableName;
    private final int timeToLiveInSeconds;
    private final boolean stronglyConsistentReads;
    private final Supplier<Instant> getTimeNow;

    private DynamoDBCompactionJobTracker(DynamoDbClient dynamoDB, InstanceProperties properties, boolean stronglyConsistentReads) {
        this(dynamoDB, properties, stronglyConsistentReads, Instant::now);
    }

    public DynamoDBCompactionJobTracker(
            DynamoDbClient dynamoDB, InstanceProperties properties, boolean stronglyConsistentReads, Supplier<Instant> getTimeNow) {
        this.dynamoDB = dynamoDB;
        this.updatesTableName = jobUpdatesTableName(properties.get(ID));
        this.jobsTableName = jobLookupTableName(properties.get(ID));
        this.timeToLiveInSeconds = properties.getInt(COMPACTION_JOB_STATUS_TTL_IN_SECONDS);
        this.stronglyConsistentReads = stronglyConsistentReads;
        this.getTimeNow = getTimeNow;
    }

    public static DynamoDBCompactionJobTracker stronglyConsistentReads(
            DynamoDbClient dynamoDB, InstanceProperties instanceProperties) {
        return new DynamoDBCompactionJobTracker(dynamoDB, instanceProperties, true);
    }

    public static DynamoDBCompactionJobTracker eventuallyConsistentReads(
            DynamoDbClient dynamoDB, InstanceProperties instanceProperties) {
        return new DynamoDBCompactionJobTracker(dynamoDB, instanceProperties, false);
    }

    public static String jobUpdatesTableName(String instanceId) {
        return instanceTableName(instanceId, "compaction-job-updates");
    }

    public static String jobLookupTableName(String instanceId) {
        return instanceTableName(instanceId, "compaction-job-lookup");
    }

    @Override
    public void jobCreated(CompactionJobCreatedEvent event) {
        try {
            save(createJobCreated(event, jobUpdateBuilder(event.getTableId(), event.getJobId())));
        } catch (RuntimeException e) {
            throw new CompactionTrackerException("Failed saving created event for job " + event.getJobId(), e);
        }
    }

    @Override
    public void jobStarted(CompactionJobStartedEvent event) {
        try {
            save(createJobStartedUpdate(event, jobUpdateBuilder(event.getTableId(), event.getJobId())));
        } catch (RuntimeException e) {
            throw new CompactionTrackerException("Failed saving started event for job " + event.getJobId(), e);
        }
    }

    @Override
    public void jobFinished(CompactionJobFinishedEvent event) {
        try {
            save(createJobFinishedUpdate(event, jobUpdateBuilder(event.getTableId(), event.getJobId())));
        } catch (RuntimeException e) {
            throw new CompactionTrackerException("Failed saving finished event for job " + event.getJobId(), e);
        }
    }

    @Override
    public void jobCommitted(CompactionJobCommittedEvent event) {
        try {
            save(createJobCommittedUpdate(event, jobUpdateBuilder(event.getTableId(), event.getJobId())));
        } catch (RuntimeException e) {
            throw new CompactionTrackerException("Failed saving committed event for job " + event.getJobId(), e);
        }
    }

    @Override
    public void jobFailed(CompactionJobFailedEvent event) {
        try {
            save(createJobFailedUpdate(event, jobUpdateBuilder(event.getTableId(), event.getJobId())));
        } catch (RuntimeException e) {
            throw new CompactionTrackerException("Failed saving failed event for job " + event.getJobId(), e);
        }
    }

    private void save(Map<String, AttributeValue> statusUpdate) {
        addStatusUpdate(statusUpdate);
        updateJobStatus(statusUpdate);
    }

    private void addStatusUpdate(Map<String, AttributeValue> statusUpdate) {
        Instant startTime = Instant.now();
        PutItemResponse result = dynamoDB.putItem(PutItemRequest.builder()
                .tableName(updatesTableName)
                .item(statusUpdate)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .build());
        LOGGER.debug("Added {} for job {}, capacity consumed = {}, took {}",
                getStringAttribute(statusUpdate, UPDATE_TYPE), getStringAttribute(statusUpdate, JOB_ID),
                result.consumedCapacity().capacityUnits(),
                LoggedDuration.withFullOutput(startTime, Instant.now()));
    }

    private void updateJobStatus(Map<String, AttributeValue> statusUpdate) {
        Instant startTime = Instant.now();
        UpdateItemResponse result = dynamoDB.updateItem(UpdateItemRequest.builder()
                .tableName(jobsTableName)
                .key(Map.of(JOB_ID, statusUpdate.get(JOB_ID)))
                .updateExpression("SET " +
                        "#Table = :table, " +
                        "#FirstUpdate = if_not_exists(#FirstUpdate, :update_time), " +
                        "#LastUpdate = :update_time, " +
                        "#LastUpdateType = :update_type, " +
                        "#Expiry = if_not_exists(#Expiry, :expiry)")
                .expressionAttributeNames(Map.of(
                        "#Table", TABLE_ID,
                        "#FirstUpdate", JOB_FIRST_UPDATE_TIME,
                        "#LastUpdate", JOB_LAST_UPDATE_TIME,
                        "#LastUpdateType", JOB_LAST_UPDATE_TYPE,
                        "#Expiry", EXPIRY_DATE))
                .expressionAttributeValues(Map.of(
                        ":table", statusUpdate.get(TABLE_ID),
                        ":update_time", statusUpdate.get(UPDATE_TIME),
                        ":update_type", statusUpdate.get(UPDATE_TYPE),
                        ":expiry", statusUpdate.get(EXPIRY_DATE)))
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .build());
        LOGGER.debug("Updated status for job {}, capacity consumed = {}, took {}",
                getStringAttribute(statusUpdate, JOB_ID),
                result.consumedCapacity().capacityUnits(),
                LoggedDuration.withFullOutput(startTime, Instant.now()));
    }

    private DynamoDBRecordBuilder jobUpdateBuilder(String tableId, String jobId) {
        Instant timeNow = getTimeNow.get();
        Instant expiry = timeNow.plus(timeToLiveInSeconds, ChronoUnit.SECONDS);
        return DynamoDBCompactionJobStatusFormat.jobUpdateBuilder(tableId, jobId, timeNow, expiry);
    }

    @Override
    public Stream<CompactionJobStatus> streamAllJobs(String tableId) {
        return DynamoDBCompactionJobStatusFormat.streamJobStatuses(streamPagedItems(dynamoDB, QueryRequest.builder()
                .tableName(updatesTableName)
                .keyConditionExpression("#TableId = :table_id")
                .expressionAttributeNames(Map.of("#TableId", TABLE_ID))
                .expressionAttributeValues(
                        Map.of(":table_id", createStringAttribute(tableId)))
                .consistentRead(stronglyConsistentReads)
                .build()));
    }

    @Override
    public Optional<CompactionJobStatus> getJob(String jobId) {
        return lookupJobTableId(jobId).flatMap(tableId -> DynamoDBCompactionJobStatusFormat
                .streamJobStatuses(streamPagedItems(dynamoDB, QueryRequest.builder()
                        .tableName(updatesTableName)
                        .keyConditionExpression("#TableId = :table_id AND begins_with(#JobAndUpdate, :job_id)")
                        .expressionAttributeNames(Map.of(
                                "#TableId", TABLE_ID,
                                "#JobAndUpdate", JOB_ID_AND_UPDATE))
                        .expressionAttributeValues(Map.of(
                                ":table_id", createStringAttribute(tableId),
                                ":job_id", createStringAttribute(jobId + "|")))
                        .consistentRead(stronglyConsistentReads)
                        .build()))
                .findFirst());
    }

    private Optional<String> lookupJobTableId(String jobId) {
        QueryResponse result = dynamoDB.query(QueryRequest.builder()
                .tableName(jobsTableName)
                .keyConditionExpression("#JobId = :job_id")
                .expressionAttributeNames(Map.of("#JobId", JOB_ID))
                .expressionAttributeValues(Map.of(":job_id", createStringAttribute(jobId)))
                .consistentRead(stronglyConsistentReads)
                .build());
        return result.items().stream()
                .map(item -> getStringAttribute(item, TABLE_ID))
                .findFirst();
    }
}
