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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.table.TableIdentity;
import sleeper.ingest.IngestStatusStoreException;
import sleeper.ingest.job.status.IngestJobFinishedEvent;
import sleeper.ingest.job.status.IngestJobStartedEvent;
import sleeper.ingest.job.status.IngestJobStatus;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.job.status.IngestJobValidatedEvent;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_JOB_STATUS_TTL_IN_SECONDS;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.instanceTableName;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;
import static sleeper.ingest.status.store.job.DynamoDBIngestJobStatusFormat.VALIDATION_REJECTED_VALUE;

public class DynamoDBIngestJobStatusStore implements IngestJobStatusStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBIngestJobStatusStore.class);
    public static final String TABLE_ID = DynamoDBIngestJobStatusFormat.TABLE_ID;
    public static final String JOB_ID = DynamoDBIngestJobStatusFormat.JOB_ID;
    public static final String JOB_UPDATES = DynamoDBIngestJobStatusFormat.JOB_UPDATES;
    public static final String LAST_VALIDATION_RESULT = DynamoDBIngestJobStatusFormat.LAST_VALIDATION_RESULT;
    public static final String EXPIRY_DATE = DynamoDBIngestJobStatusFormat.EXPIRY_DATE;
    public static final String JOB_INDEX = "by-job-id";
    public static final String INVALID_INDEX = "by-invalid";

    private final AmazonDynamoDB dynamoDB;
    private final String statusTableName;
    private final DynamoDBIngestJobStatusFormat format;

    DynamoDBIngestJobStatusStore(AmazonDynamoDB dynamoDB, InstanceProperties properties, Supplier<Instant> getTimeNow) {
        this.dynamoDB = dynamoDB;
        this.statusTableName = jobStatusTableName(properties.get(ID));
        int timeToLiveInSeconds = properties.getInt(INGEST_JOB_STATUS_TTL_IN_SECONDS);
        this.format = new DynamoDBIngestJobStatusFormat(timeToLiveInSeconds, getTimeNow);
    }

    public static String jobStatusTableName(String instanceId) {
        return instanceTableName(instanceId, "ingest-job-status");
    }

    @Override
    public void jobValidated(IngestJobValidatedEvent event) {
        try {
            UpdateItemResult result = updateItem(
                    format.createKeyWithTableAndJob(event.getTableId(), event.getJobId()),
                    format.createJobValidatedUpdates(event));
            LOGGER.info("Added validated event for job {} in table {}, capacity consumed = {}",
                    event.getJobId(), statusTableName, result.getConsumedCapacity().getCapacityUnits());
        } catch (RuntimeException e) {
            throw new IngestStatusStoreException("Failed updateItem in jobValidated for job " + event.getJobId(), e);
        }
    }

    @Override
    public void jobStarted(IngestJobStartedEvent event) {
        try {
            UpdateItemResult result = updateItem(
                    format.createKeyWithTableAndJob(event.getTableId(), event.getJobId()),
                    format.createJobStartedUpdates(event));
            LOGGER.info("Added started event for job {} in table {}, capacity consumed = {}",
                    event.getJobId(), statusTableName, result.getConsumedCapacity().getCapacityUnits());
        } catch (RuntimeException e) {
            throw new IngestStatusStoreException("Failed updateItem in jobStarted for job " + event.getJobId(), e);
        }
    }

    @Override
    public void jobFinished(IngestJobFinishedEvent event) {
        try {
            UpdateItemResult result = updateItem(
                    format.createKeyWithTableAndJob(event.getTableId(), event.getJobId()),
                    format.createJobFinishedUpdates(event));
            LOGGER.info("Added finished event for job {} to table {}, capacity consumed = {}",
                    event.getJobId(), statusTableName, result.getConsumedCapacity().getCapacityUnits());
        } catch (RuntimeException e) {
            throw new IngestStatusStoreException("Failed updateItem in jobFinished for job " + event.getJobId(), e);
        }
    }

    private UpdateItemResult updateItem(Map<String, AttributeValue> key, Map<String, AttributeValueUpdate> updates) {
        return dynamoDB.updateItem(new UpdateItemRequest()
                .withTableName(statusTableName)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withKey(key).withAttributeUpdates(updates));
    }

    @Override
    public Optional<IngestJobStatus> getJob(String jobId) {
        return DynamoDBIngestJobStatusFormat.streamJobStatuses(streamPagedItems(dynamoDB,
                new QueryRequest()
                        .withTableName(statusTableName).withIndexName(JOB_INDEX)
                        .withKeyConditionExpression("#JobId = :job_id")
                        .withExpressionAttributeNames(Map.of("#JobId", JOB_ID))
                        .withExpressionAttributeValues(Map.of(":job_id", createStringAttribute(jobId)))
        )).findFirst();
    }

    @Override
    public Stream<IngestJobStatus> streamAllJobs(TableIdentity tableId) {
        return DynamoDBIngestJobStatusFormat.streamJobStatuses(
                streamPagedItems(dynamoDB, new QueryRequest()
                        .withTableName(statusTableName)
                        .withKeyConditionExpression("#TableId = :table_id")
                        .withExpressionAttributeNames(Map.of("#TableId", TABLE_ID))
                        .withExpressionAttributeValues(
                                Map.of(":table_id", createStringAttribute(tableId.getTableUniqueId())))));
    }

    @Override
    public List<IngestJobStatus> getInvalidJobs() {
        return DynamoDBIngestJobStatusFormat.streamJobStatuses(
                streamPagedItems(dynamoDB, new QueryRequest()
                        .withTableName(statusTableName).withIndexName(INVALID_INDEX)
                        .withKeyConditionExpression("#ValidationRejected = :rejected")
                        .withExpressionAttributeNames(Map.of("#ValidationRejected", LAST_VALIDATION_RESULT))
                        .withExpressionAttributeValues(Map.of(":rejected", createStringAttribute(VALIDATION_REJECTED_VALUE))))
        ).collect(Collectors.toUnmodifiableList());
    }
}
