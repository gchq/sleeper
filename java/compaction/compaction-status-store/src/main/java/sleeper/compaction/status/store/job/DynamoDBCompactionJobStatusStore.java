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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.status.store.CompactionStatusStoreException;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.table.TableIdentity;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_JOB_STATUS_TTL_IN_SECONDS;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.instanceTableName;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;

public class DynamoDBCompactionJobStatusStore implements CompactionJobStatusStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBCompactionJobStatusStore.class);
    public static final String TABLE_ID = DynamoDBCompactionJobStatusFormat.TABLE_ID;
    public static final String JOB_ID = DynamoDBCompactionJobStatusFormat.JOB_ID;
    public static final String JOB_ID_AND_TIME = DynamoDBCompactionJobStatusFormat.JOB_ID_AND_TIME;
    public static final String EXPIRY_DATE = DynamoDBCompactionJobStatusFormat.EXPIRY_DATE;
    public static final String JOB_INDEX = "by-job-id";

    private final AmazonDynamoDB dynamoDB;
    private final String statusTableName;
    private final DynamoDBCompactionJobStatusFormat format;

    public DynamoDBCompactionJobStatusStore(AmazonDynamoDB dynamoDB, InstanceProperties properties) {
        this(dynamoDB, properties, Instant::now);
    }

    public DynamoDBCompactionJobStatusStore(
            AmazonDynamoDB dynamoDB, InstanceProperties properties, Supplier<Instant> getTimeNow) {
        this.dynamoDB = dynamoDB;
        this.statusTableName = jobStatusTableName(properties.get(ID));
        int timeToLiveInSeconds = properties.getInt(COMPACTION_JOB_STATUS_TTL_IN_SECONDS);
        format = new DynamoDBCompactionJobStatusFormat(timeToLiveInSeconds, getTimeNow);
    }

    public static String jobStatusTableName(String instanceId) {
        return instanceTableName(instanceId, "compaction-job-status");
    }

    @Override
    public void jobCreated(CompactionJob job) {
        try {
            PutItemResult result = putItem(format.createJobCreatedRecord(job));
            LOGGER.info("Put created event for job {} to table {}, capacity consumed = {}",
                    job.getId(), statusTableName, result.getConsumedCapacity().getCapacityUnits());
        } catch (RuntimeException e) {
            throw new CompactionStatusStoreException("Failed putItem in jobCreated for job " + job.getId(), e);
        }
    }

    @Override
    public void jobStarted(CompactionJob job, Instant startTime, String taskId) {
        try {
            PutItemResult result = putItem(format.createJobStartedRecord(job, startTime, taskId));
            LOGGER.info("Put started event for job {} to table {}, capacity consumed = {}",
                    job.getId(), statusTableName, result.getConsumedCapacity().getCapacityUnits());
        } catch (RuntimeException e) {
            throw new CompactionStatusStoreException("Failed putItem in jobStarted for job " + job.getId(), e);
        }
    }

    @Override
    public void jobFinished(CompactionJob job, RecordsProcessedSummary summary, String taskId) {
        try {
            PutItemResult result = putItem(format.createJobFinishedRecord(job, summary, taskId));
            LOGGER.info("Put finished event for job {} to table {}, capacity consumed = {}",
                    job.getId(), statusTableName, result.getConsumedCapacity().getCapacityUnits());
        } catch (RuntimeException e) {
            throw new CompactionStatusStoreException("Failed putItem in jobFinished for job " + job.getId(), e);
        }
    }

    private PutItemResult putItem(Map<String, AttributeValue> item) {
        PutItemRequest putItemRequest = new PutItemRequest()
                .withItem(item)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withTableName(statusTableName);
        return dynamoDB.putItem(putItemRequest);
    }

    @Override
    public Optional<CompactionJobStatus> getJob(String jobId) {
        return getJobStream(jobId).findFirst();
    }

    @Override
    public Stream<CompactionJobStatus> streamAllJobs(TableIdentity tableId) {
        return DynamoDBCompactionJobStatusFormat.streamJobStatuses(
                streamPagedItems(dynamoDB, new QueryRequest()
                        .withTableName(statusTableName)
                        .withKeyConditionExpression("#TableId = :table_id")
                        .withExpressionAttributeNames(Map.of("#TableId", TABLE_ID))
                        .withExpressionAttributeValues(
                                Map.of(":table_id", createStringAttribute(tableId.getTableUniqueId())))));
    }

    private Stream<CompactionJobStatus> getJobStream(String jobId) {
        QueryResult result = dynamoDB.query(new QueryRequest()
                .withTableName(statusTableName).withIndexName(JOB_INDEX)
                .withKeyConditionExpression("#JobId = :job_id")
                .withExpressionAttributeNames(Map.of("#JobId", JOB_ID))
                .withExpressionAttributeValues(Map.of(":job_id", createStringAttribute(jobId))));
        return DynamoDBCompactionJobStatusFormat.streamJobStatuses(result.getItems().stream()
                .map(indexItem -> indexItem.get(TABLE_ID).getS())
                .flatMap(tableId -> streamJobItems(jobId, tableId)));
    }

    private Stream<Map<String, AttributeValue>> streamJobItems(String jobId, String tableId) {
        return streamPagedItems(dynamoDB, new QueryRequest()
                .withTableName(statusTableName)
                .withKeyConditionExpression("#TableId = :table_id AND begins_with(#JobIdAndTime, :job_id)")
                .withExpressionAttributeNames(Map.of("#TableId", TABLE_ID, "#JobIdAndTime", JOB_ID_AND_TIME))
                .withExpressionAttributeValues(Map.of(
                        ":table_id", createStringAttribute(tableId),
                        ":job_id", createStringAttribute(jobId + "|"))));
    }
}
