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
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.status.store.CompactionStatusStoreException;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.record.process.RecordsProcessedSummary;

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
            LOGGER.debug("Put created event for job {} to table {}, capacity consumed = {}",
                    job.getId(), statusTableName, result.getConsumedCapacity().getCapacityUnits());
        } catch (RuntimeException e) {
            throw new CompactionStatusStoreException("Failed putItem in jobCreated", e);
        }
    }

    @Override
    public void jobStarted(CompactionJob job, Instant startTime, String taskId) {
        try {
            PutItemResult result = putItem(format.createJobStartedRecord(job, startTime, taskId));
            LOGGER.debug("Put started event for job {} to table {}, capacity consumed = {}",
                    job.getId(), statusTableName, result.getConsumedCapacity().getCapacityUnits());
        } catch (RuntimeException e) {
            throw new CompactionStatusStoreException("Failed putItem in jobStarted", e);
        }
    }

    @Override
    public void jobFinished(CompactionJob job, RecordsProcessedSummary summary, String taskId) {
        try {
            PutItemResult result = putItem(format.createJobFinishedRecord(job, summary, taskId));
            LOGGER.debug("Put finished event for job {} to table {}, capacity consumed = {}",
                    job.getId(), statusTableName, result.getConsumedCapacity().getCapacityUnits());
        } catch (RuntimeException e) {
            throw new CompactionStatusStoreException("Failed putItem in jobFinished", e);
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

    private Stream<CompactionJobStatus> getJobStream(String jobId) {
        QueryResult result = dynamoDB.query(new QueryRequest()
                .withTableName(statusTableName)
                .addKeyConditionsEntry(DynamoDBCompactionJobStatusFormat.JOB_ID, new Condition()
                        .withAttributeValueList(createStringAttribute(jobId))
                        .withComparisonOperator(ComparisonOperator.EQ)));
        return DynamoDBCompactionJobStatusFormat.streamJobStatuses(result.getItems().stream());
    }

    @Override
    public Stream<CompactionJobStatus> streamAllJobs(String tableName) {
        return DynamoDBCompactionJobStatusFormat.streamJobStatuses(
                streamPagedItems(dynamoDB, createScanRequestByTable(tableName)));
    }

    private ScanRequest createScanRequestByTable(String tableName) {
        return new ScanRequest()
                .withTableName(statusTableName)
                .addScanFilterEntry(DynamoDBCompactionJobStatusFormat.TABLE_NAME, new Condition()
                        .withAttributeValueList(createStringAttribute(tableName))
                        .withComparisonOperator(ComparisonOperator.EQ));
    }
}
