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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.util.LoggedDuration;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;
import sleeper.ingest.IngestStatusStoreException;
import sleeper.ingest.job.status.IngestJobAddedFilesEvent;
import sleeper.ingest.job.status.IngestJobFailedEvent;
import sleeper.ingest.job.status.IngestJobFinishedEvent;
import sleeper.ingest.job.status.IngestJobStartedEvent;
import sleeper.ingest.job.status.IngestJobStatus;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.job.status.IngestJobValidatedEvent;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_JOB_STATUS_TTL_IN_SECONDS;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.instanceTableName;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;
import static sleeper.ingest.status.store.job.DynamoDBIngestJobStatusFormat.UPDATE_TIME;
import static sleeper.ingest.status.store.job.DynamoDBIngestJobStatusFormat.UPDATE_TYPE;
import static sleeper.ingest.status.store.job.DynamoDBIngestJobStatusFormat.VALIDATION_REJECTED_VALUE;
import static sleeper.ingest.status.store.job.DynamoDBIngestJobStatusFormat.VALIDATION_RESULT;
import static sleeper.ingest.status.store.job.DynamoDBIngestJobStatusFormat.createJobAddedFilesUpdate;
import static sleeper.ingest.status.store.job.DynamoDBIngestJobStatusFormat.createJobFailedUpdate;
import static sleeper.ingest.status.store.job.DynamoDBIngestJobStatusFormat.createJobFinishedUpdate;
import static sleeper.ingest.status.store.job.DynamoDBIngestJobStatusFormat.createJobStartedUpdate;
import static sleeper.ingest.status.store.job.DynamoDBIngestJobStatusFormat.createJobValidatedUpdate;

public class DynamoDBIngestJobStatusStore implements IngestJobStatusStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBIngestJobStatusStore.class);
    public static final String TABLE_ID = DynamoDBIngestJobStatusFormat.TABLE_ID;
    public static final String JOB_ID = DynamoDBIngestJobStatusFormat.JOB_ID;
    public static final String JOB_ID_AND_UPDATE = DynamoDBIngestJobStatusFormat.JOB_ID_AND_UPDATE;
    public static final String EXPIRY_DATE = DynamoDBIngestJobStatusFormat.EXPIRY_DATE;
    private static final String JOB_FIRST_UPDATE_TIME = "FirstUpdateTime";
    private static final String JOB_LAST_UPDATE_TIME = "LastUpdateTime";
    private static final String JOB_LAST_UPDATE_TYPE = "LastUpdateType";
    public static final String JOB_LAST_VALIDATION_RESULT = "LastValidationResult";
    public static final String VALIDATION_INDEX = "by-validation";

    private final AmazonDynamoDB dynamoDB;
    private final String updatesTableName;
    private final String jobsTableName;
    private final int timeToLiveInSeconds;
    private final Supplier<Instant> getTimeNow;

    DynamoDBIngestJobStatusStore(AmazonDynamoDB dynamoDB, InstanceProperties properties, Supplier<Instant> getTimeNow) {
        this.dynamoDB = dynamoDB;
        this.updatesTableName = jobUpdatesTableName(properties.get(ID));
        this.jobsTableName = jobLookupTableName(properties.get(ID));
        this.timeToLiveInSeconds = properties.getInt(INGEST_JOB_STATUS_TTL_IN_SECONDS);
        this.getTimeNow = getTimeNow;
    }

    public static String jobUpdatesTableName(String instanceId) {
        return instanceTableName(instanceId, "ingest-job-updates");
    }

    public static String jobLookupTableName(String instanceId) {
        return instanceTableName(instanceId, "ingest-job-lookup");
    }

    @Override
    public void jobValidated(IngestJobValidatedEvent event) {
        try {
            save(createJobValidatedUpdate(event, jobUpdateBuilder(event.getTableId(), event.getJobId())));
        } catch (RuntimeException e) {
            throw new IngestStatusStoreException("Failed saving validated event for job " + event.getJobId(), e);
        }
    }

    @Override
    public void jobStarted(IngestJobStartedEvent event) {
        try {
            save(createJobStartedUpdate(event, jobUpdateBuilder(event.getTableId(), event.getJobId())));
        } catch (RuntimeException e) {
            throw new IngestStatusStoreException("Failed saving started event for job " + event.getJobId(), e);
        }
    }

    @Override
    public void jobAddedFiles(IngestJobAddedFilesEvent event) {
        try {
            save(createJobAddedFilesUpdate(event, jobUpdateBuilder(event.getTableId(), event.getJobId())));
        } catch (RuntimeException e) {
            throw new IngestStatusStoreException("Failed saving added files event for job " + event.getJobId(), e);
        }
    }

    @Override
    public void jobFinished(IngestJobFinishedEvent event) {
        try {
            save(createJobFinishedUpdate(event, jobUpdateBuilder(event.getTableId(), event.getJobId())));
        } catch (RuntimeException e) {
            throw new IngestStatusStoreException("Failed saving finished event for job " + event.getJobId(), e);
        }
    }

    @Override
    public void jobFailed(IngestJobFailedEvent event) {
        try {
            save(createJobFailedUpdate(event, jobUpdateBuilder(event.getTableId(), event.getJobId())));
        } catch (RuntimeException e) {
            throw new IngestStatusStoreException("Failed saving failed event for job " + event.getJobId(), e);
        }
    }

    private void save(Map<String, AttributeValue> statusUpdate) {
        Instant startTime = Instant.now();
        String updateExpression = "SET " +
                "#Table = :table, " +
                "#FirstUpdate = if_not_exists(#FirstUpdate, :update_time), " +
                "#LastUpdate = :update_time, " +
                "#LastUpdateType = :update_type, " +
                "#Expiry = if_not_exists(#Expiry, :expiry)";
        Map<String, String> expressionAttributeNames = Map.of(
                "#Table", TABLE_ID,
                "#FirstUpdate", JOB_FIRST_UPDATE_TIME,
                "#LastUpdate", JOB_LAST_UPDATE_TIME,
                "#LastUpdateType", JOB_LAST_UPDATE_TYPE,
                "#Expiry", EXPIRY_DATE);
        Map<String, AttributeValue> expressionAttributeValues = Map.of(
                ":table", statusUpdate.get(TABLE_ID),
                ":update_time", statusUpdate.get(UPDATE_TIME),
                ":update_type", statusUpdate.get(UPDATE_TYPE),
                ":expiry", statusUpdate.get(EXPIRY_DATE));
        AttributeValue validationResult = statusUpdate.get(VALIDATION_RESULT);
        if (validationResult != null) {
            updateExpression += ", #LastValidationResult = :validation_result";
            expressionAttributeNames = new HashMap<>(expressionAttributeNames);
            expressionAttributeNames.put("#LastValidationResult", JOB_LAST_VALIDATION_RESULT);
            expressionAttributeValues = new HashMap<>(expressionAttributeValues);
            expressionAttributeValues.put(":validation_result", validationResult);
        }
        PutItemResult putResult = dynamoDB.putItem(new PutItemRequest()
                .withTableName(updatesTableName)
                .withItem(statusUpdate)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL));
        Instant afterPutTime = Instant.now();
        LOGGER.debug("Added {} for job {}, capacity consumed = {}, took {}",
                getStringAttribute(statusUpdate, UPDATE_TYPE), getStringAttribute(statusUpdate, JOB_ID),
                putResult.getConsumedCapacity().getCapacityUnits(),
                LoggedDuration.withFullOutput(startTime, afterPutTime));
        UpdateItemResult updateResult = dynamoDB.updateItem(new UpdateItemRequest()
                .withTableName(jobsTableName)
                .withKey(Map.of(JOB_ID, statusUpdate.get(JOB_ID)))
                .withUpdateExpression(updateExpression)
                .withExpressionAttributeNames(expressionAttributeNames)
                .withExpressionAttributeValues(expressionAttributeValues)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL));
        LOGGER.debug("Updated status for job {}, capacity consumed = {}, took {}",
                getStringAttribute(statusUpdate, JOB_ID),
                updateResult.getConsumedCapacity().getCapacityUnits(),
                LoggedDuration.withFullOutput(startTime, afterPutTime));
    }

    private DynamoDBRecordBuilder jobUpdateBuilder(String tableId, String jobId) {
        Instant timeNow = getTimeNow.get();
        Instant expiry = timeNow.plus(timeToLiveInSeconds, ChronoUnit.SECONDS);
        return DynamoDBIngestJobStatusFormat.jobUpdateBuilder(tableId, jobId, timeNow, expiry);
    }

    @Override
    public Stream<IngestJobStatus> streamAllJobs(String tableId) {
        return DynamoDBIngestJobStatusFormat.streamJobStatuses(
                streamPagedItems(dynamoDB, new QueryRequest()
                        .withTableName(updatesTableName)
                        .withKeyConditionExpression("#TableId = :table_id")
                        .withExpressionAttributeNames(Map.of("#TableId", TABLE_ID))
                        .withExpressionAttributeValues(
                                Map.of(":table_id", createStringAttribute(tableId)))));
    }

    @Override
    public List<IngestJobStatus> getInvalidJobs() {
        return DynamoDBIngestJobStatusFormat.streamJobStatuses(
                lookupInvalidJobs().flatMap(this::loadStatusItems)).collect(Collectors.toUnmodifiableList());
    }

    @Override
    public Optional<IngestJobStatus> getJob(String jobId) {
        return lookupJobById(jobId)
                .flatMap(jobItem -> DynamoDBIngestJobStatusFormat.streamJobStatuses(loadStatusItems(jobItem)).findFirst());
    }

    private Optional<Map<String, AttributeValue>> lookupJobById(String jobId) {
        QueryResult result = dynamoDB.query(new QueryRequest()
                .withTableName(jobsTableName)
                .withKeyConditionExpression("#JobId = :job_id")
                .withExpressionAttributeNames(Map.of("#JobId", JOB_ID))
                .withExpressionAttributeValues(Map.of(":job_id", createStringAttribute(jobId))));
        return result.getItems().stream().findFirst();
    }

    private Stream<Map<String, AttributeValue>> lookupInvalidJobs() {
        return streamPagedItems(dynamoDB, new QueryRequest()
                .withTableName(jobsTableName).withIndexName(VALIDATION_INDEX)
                .withKeyConditionExpression("#Result = :rejected")
                .withExpressionAttributeNames(Map.of("#Result", JOB_LAST_VALIDATION_RESULT))
                .withExpressionAttributeValues(Map.of(":rejected", createStringAttribute(VALIDATION_REJECTED_VALUE))));
    }

    private Stream<Map<String, AttributeValue>> loadStatusItems(Map<String, AttributeValue> jobLookupItem) {
        return streamPagedItems(dynamoDB, new QueryRequest()
                .withTableName(updatesTableName)
                .withKeyConditionExpression("#TableId = :table_id AND begins_with(#JobAndUpdate, :job_id)")
                .withExpressionAttributeNames(Map.of(
                        "#TableId", TABLE_ID,
                        "#JobAndUpdate", JOB_ID_AND_UPDATE))
                .withExpressionAttributeValues(Map.of(
                        ":table_id", jobLookupItem.get(TABLE_ID),
                        ":job_id", createStringAttribute(getStringAttribute(jobLookupItem, JOB_ID) + "|"))));
    }
}
