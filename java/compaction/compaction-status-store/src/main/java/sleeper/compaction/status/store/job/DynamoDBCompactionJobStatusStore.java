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
package sleeper.compaction.status.store.job;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.CompactionJobStatusStore;
import sleeper.compaction.core.job.status.CompactionJobCommittedEvent;
import sleeper.compaction.core.job.status.CompactionJobCreatedEvent;
import sleeper.compaction.core.job.status.CompactionJobFailedEvent;
import sleeper.compaction.core.job.status.CompactionJobFinishedEvent;
import sleeper.compaction.core.job.status.CompactionJobStartedEvent;
import sleeper.compaction.core.job.status.CompactionJobStatus;
import sleeper.compaction.status.store.CompactionStatusStoreException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.util.LoggedDuration;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static sleeper.compaction.status.store.job.DynamoDBCompactionJobStatusFormat.UPDATE_TIME;
import static sleeper.compaction.status.store.job.DynamoDBCompactionJobStatusFormat.createFilesAssignedUpdate;
import static sleeper.compaction.status.store.job.DynamoDBCompactionJobStatusFormat.createJobCommittedUpdate;
import static sleeper.compaction.status.store.job.DynamoDBCompactionJobStatusFormat.createJobCreated;
import static sleeper.compaction.status.store.job.DynamoDBCompactionJobStatusFormat.createJobFailedUpdate;
import static sleeper.compaction.status.store.job.DynamoDBCompactionJobStatusFormat.createJobFinishedUpdate;
import static sleeper.compaction.status.store.job.DynamoDBCompactionJobStatusFormat.createJobStartedUpdate;
import static sleeper.compaction.status.store.task.DynamoDBCompactionTaskStatusFormat.UPDATE_TYPE;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_STATUS_TTL_IN_SECONDS;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.instanceTableName;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;

public class DynamoDBCompactionJobStatusStore implements CompactionJobStatusStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBCompactionJobStatusStore.class);
    public static final String TABLE_ID = DynamoDBCompactionJobStatusFormat.TABLE_ID;
    public static final String JOB_ID = DynamoDBCompactionJobStatusFormat.JOB_ID;
    public static final String JOB_ID_AND_UPDATE = DynamoDBCompactionJobStatusFormat.JOB_ID_AND_UPDATE;
    public static final String EXPIRY_DATE = DynamoDBCompactionJobStatusFormat.EXPIRY_DATE;
    private static final String JOB_FIRST_UPDATE_TIME = "FirstUpdateTime";
    private static final String JOB_LAST_UPDATE_TIME = "LastUpdateTime";
    private static final String JOB_LAST_UPDATE_TYPE = "LastUpdateType";

    private final AmazonDynamoDB dynamoDB;
    private final String updatesTableName;
    private final String jobsTableName;
    private final int timeToLiveInSeconds;
    private final boolean stronglyConsistentReads;
    private final Supplier<Instant> getTimeNow;

    private DynamoDBCompactionJobStatusStore(AmazonDynamoDB dynamoDB, InstanceProperties properties, boolean stronglyConsistentReads) {
        this(dynamoDB, properties, stronglyConsistentReads, Instant::now);
    }

    public DynamoDBCompactionJobStatusStore(
            AmazonDynamoDB dynamoDB, InstanceProperties properties, boolean stronglyConsistentReads, Supplier<Instant> getTimeNow) {
        this.dynamoDB = dynamoDB;
        this.updatesTableName = jobUpdatesTableName(properties.get(ID));
        this.jobsTableName = jobLookupTableName(properties.get(ID));
        this.timeToLiveInSeconds = properties.getInt(COMPACTION_JOB_STATUS_TTL_IN_SECONDS);
        this.stronglyConsistentReads = stronglyConsistentReads;
        this.getTimeNow = getTimeNow;
    }

    public static DynamoDBCompactionJobStatusStore stronglyConsistentReads(
            AmazonDynamoDB dynamoDB, InstanceProperties instanceProperties) {
        return new DynamoDBCompactionJobStatusStore(dynamoDB, instanceProperties, true);
    }

    public static DynamoDBCompactionJobStatusStore eventuallyConsistentReads(
            AmazonDynamoDB dynamoDB, InstanceProperties instanceProperties) {
        return new DynamoDBCompactionJobStatusStore(dynamoDB, instanceProperties, false);
    }

    public static String jobUpdatesTableName(String instanceId) {
        return instanceTableName(instanceId, "compaction-job-updates");
    }

    public static String jobLookupTableName(String instanceId) {
        return instanceTableName(instanceId, "compaction-job-lookup");
    }

    @Override
    public void jobInputFilesAssigned(String tableId, List<AssignJobIdRequest> requests) {
        for (AssignJobIdRequest request : requests) {
            try {
                save(createFilesAssignedUpdate(request, jobUpdateBuilder(tableId, request.getJobId())));
            } catch (RuntimeException e) {
                throw new CompactionStatusStoreException("Failed saving input files assigned event for job " + request.getJobId(), e);
            }
        }
    }

    @Override
    public void jobCreated(CompactionJobCreatedEvent event) {
        try {
            save(createJobCreated(event, jobUpdateBuilder(event.getTableId(), event.getJobId())));
        } catch (RuntimeException e) {
            throw new CompactionStatusStoreException("Failed saving created event for job " + event.getJobId(), e);
        }
    }

    @Override
    public void jobStarted(CompactionJobStartedEvent event) {
        try {
            save(createJobStartedUpdate(event, jobUpdateBuilder(event.getTableId(), event.getJobId())));
        } catch (RuntimeException e) {
            throw new CompactionStatusStoreException("Failed saving started event for job " + event.getJobId(), e);
        }
    }

    @Override
    public void jobFinished(CompactionJobFinishedEvent event) {
        try {
            save(createJobFinishedUpdate(event, jobUpdateBuilder(event.getTableId(), event.getJobId())));
        } catch (RuntimeException e) {
            throw new CompactionStatusStoreException("Failed saving finished event for job " + event.getJobId(), e);
        }
    }

    @Override
    public void jobCommitted(CompactionJobCommittedEvent event) {
        try {
            save(createJobCommittedUpdate(event, jobUpdateBuilder(event.getTableId(), event.getJobId())));
        } catch (RuntimeException e) {
            throw new CompactionStatusStoreException("Failed saving committed event for job " + event.getJobId(), e);
        }
    }

    @Override
    public void jobFailed(CompactionJobFailedEvent event) {
        try {
            save(createJobFailedUpdate(event, jobUpdateBuilder(event.getTableId(), event.getJobId())));
        } catch (RuntimeException e) {
            throw new CompactionStatusStoreException("Failed saving failed event for job " + event.getJobId(), e);
        }
    }

    private void save(Map<String, AttributeValue> statusUpdate) {
        addStatusUpdate(statusUpdate);
        updateJobStatus(statusUpdate);
    }

    private void addStatusUpdate(Map<String, AttributeValue> statusUpdate) {
        Instant startTime = Instant.now();
        PutItemResult result = dynamoDB.putItem(new PutItemRequest()
                .withTableName(updatesTableName)
                .withItem(statusUpdate)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL));
        LOGGER.debug("Added {} for job {}, capacity consumed = {}, took {}",
                getStringAttribute(statusUpdate, UPDATE_TYPE), getStringAttribute(statusUpdate, JOB_ID),
                result.getConsumedCapacity().getCapacityUnits(),
                LoggedDuration.withFullOutput(startTime, Instant.now()));
    }

    private void updateJobStatus(Map<String, AttributeValue> statusUpdate) {
        Instant startTime = Instant.now();
        UpdateItemResult result = dynamoDB.updateItem(new UpdateItemRequest()
                .withTableName(jobsTableName)
                .withKey(Map.of(JOB_ID, statusUpdate.get(JOB_ID)))
                .withUpdateExpression("SET " +
                        "#Table = :table, " +
                        "#FirstUpdate = if_not_exists(#FirstUpdate, :update_time), " +
                        "#LastUpdate = :update_time, " +
                        "#LastUpdateType = :update_type, " +
                        "#Expiry = if_not_exists(#Expiry, :expiry)")
                .withExpressionAttributeNames(Map.of(
                        "#Table", TABLE_ID,
                        "#FirstUpdate", JOB_FIRST_UPDATE_TIME,
                        "#LastUpdate", JOB_LAST_UPDATE_TIME,
                        "#LastUpdateType", JOB_LAST_UPDATE_TYPE,
                        "#Expiry", EXPIRY_DATE))
                .withExpressionAttributeValues(Map.of(
                        ":table", statusUpdate.get(TABLE_ID),
                        ":update_time", statusUpdate.get(UPDATE_TIME),
                        ":update_type", statusUpdate.get(UPDATE_TYPE),
                        ":expiry", statusUpdate.get(EXPIRY_DATE)))
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL));
        LOGGER.debug("Updated status for job {}, capacity consumed = {}, took {}",
                getStringAttribute(statusUpdate, JOB_ID),
                result.getConsumedCapacity().getCapacityUnits(),
                LoggedDuration.withFullOutput(startTime, Instant.now()));
    }

    private DynamoDBRecordBuilder jobUpdateBuilder(String tableId, String jobId) {
        Instant timeNow = getTimeNow.get();
        Instant expiry = timeNow.plus(timeToLiveInSeconds, ChronoUnit.SECONDS);
        return DynamoDBCompactionJobStatusFormat.jobUpdateBuilder(tableId, jobId, timeNow, expiry);
    }

    @Override
    public Stream<CompactionJobStatus> streamAllJobs(String tableId) {
        return DynamoDBCompactionJobStatusFormat.streamJobStatuses(streamPagedItems(dynamoDB, new QueryRequest()
                .withTableName(updatesTableName)
                .withKeyConditionExpression("#TableId = :table_id")
                .withExpressionAttributeNames(Map.of("#TableId", TABLE_ID))
                .withExpressionAttributeValues(
                        Map.of(":table_id", createStringAttribute(tableId)))
                .withConsistentRead(stronglyConsistentReads)));
    }

    @Override
    public Optional<CompactionJobStatus> getJob(String jobId) {
        return lookupJobTableId(jobId).flatMap(tableId -> DynamoDBCompactionJobStatusFormat
                .streamJobStatuses(streamPagedItems(dynamoDB, new QueryRequest()
                        .withTableName(updatesTableName)
                        .withKeyConditionExpression("#TableId = :table_id AND begins_with(#JobAndUpdate, :job_id)")
                        .withExpressionAttributeNames(Map.of(
                                "#TableId", TABLE_ID,
                                "#JobAndUpdate", JOB_ID_AND_UPDATE))
                        .withExpressionAttributeValues(Map.of(
                                ":table_id", createStringAttribute(tableId),
                                ":job_id", createStringAttribute(jobId + "|")))
                        .withConsistentRead(stronglyConsistentReads)))
                .findFirst());
    }

    private Optional<String> lookupJobTableId(String jobId) {
        QueryResult result = dynamoDB.query(new QueryRequest()
                .withTableName(jobsTableName)
                .withKeyConditionExpression("#JobId = :job_id")
                .withExpressionAttributeNames(Map.of("#JobId", JOB_ID))
                .withExpressionAttributeValues(Map.of(":job_id", createStringAttribute(jobId)))
                .withConsistentRead(stronglyConsistentReads));
        return result.getItems().stream()
                .map(item -> getStringAttribute(item, TABLE_ID))
                .findFirst();
    }
}
