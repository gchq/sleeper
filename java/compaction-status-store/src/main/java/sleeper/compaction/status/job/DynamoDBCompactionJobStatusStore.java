/*
 * Copyright 2022 Crown Copyright
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
package sleeper.compaction.status.job;

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
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.CompactionJobSummary;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.status.CompactionStatusStoreException;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.UserDefinedInstanceProperty;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static sleeper.compaction.status.DynamoDBAttributes.createStringAttribute;
import static sleeper.compaction.status.DynamoDBUtils.instanceTableName;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.JOB_ID;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.TABLE_NAME;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_STATUS_STORE_ENABLED;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;

public class DynamoDBCompactionJobStatusStore implements CompactionJobStatusStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBCompactionJobStatusStore.class);

    private final AmazonDynamoDB dynamoDB;
    private final String statusTableName;
    private Long timeToLive;

    private DynamoDBCompactionJobStatusStore(AmazonDynamoDB dynamoDB, InstanceProperties properties) {
        this.dynamoDB = dynamoDB;
        this.statusTableName = jobStatusTableName(properties.get(ID));
        this.timeToLive = properties.getLong(UserDefinedInstanceProperty.COMPACTION_JOB_STATUS_TTL_IN_SECONDS) * 1000;
    }

    public static CompactionJobStatusStore from(AmazonDynamoDB dynamoDB, InstanceProperties properties) {
        if (Boolean.TRUE.equals(properties.getBoolean(COMPACTION_STATUS_STORE_ENABLED))) {
            return new DynamoDBCompactionJobStatusStore(dynamoDB, properties);
        } else {
            return CompactionJobStatusStore.none();
        }
    }

    public static String jobStatusTableName(String instanceId) {
        return instanceTableName(instanceId, "compaction-job-status");
    }

    @Override
    public void jobCreated(CompactionJob job) {
        try {
            PutItemResult result = putItem(DynamoDBCompactionJobStatusFormat.createJobCreatedRecord(job, timeToLive));
            LOGGER.debug("Put created event for job {} to table {}, capacity consumed = {}",
                    job.getId(), statusTableName, result.getConsumedCapacity().getCapacityUnits());
        } catch (RuntimeException e) {
            throw new CompactionStatusStoreException("Failed putItem in jobCreated", e);
        }
    }

    @Override
    public void jobStarted(CompactionJob job, Instant startTime, String taskId) {
        try {
            PutItemResult result = putItem(DynamoDBCompactionJobStatusFormat.createJobStartedRecord(job, startTime, taskId, timeToLive));
            LOGGER.debug("Put started event for job {} to table {}, capacity consumed = {}",
                    job.getId(), statusTableName, result.getConsumedCapacity().getCapacityUnits());
        } catch (RuntimeException e) {
            throw new CompactionStatusStoreException("Failed putItem in jobStarted", e);
        }
    }

    @Override
    public void jobFinished(CompactionJob job, CompactionJobSummary summary, String taskId) {
        try {
            PutItemResult result = putItem(DynamoDBCompactionJobStatusFormat.createJobFinishedRecord(job, summary, taskId, timeToLive));
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
    public CompactionJobStatus getJob(String jobId) {
        QueryResult result = dynamoDB.query(new QueryRequest()
                .withTableName(statusTableName)
                .addKeyConditionsEntry(JOB_ID, new Condition()
                        .withAttributeValueList(createStringAttribute(jobId))
                        .withComparisonOperator(ComparisonOperator.EQ)));
        return DynamoDBCompactionJobStatusFormat.streamJobStatuses(result.getItems())
                .findFirst().orElse(null);
    }

    @Override
    public List<CompactionJobStatus> getJobsByTaskId(String tableName, String taskId) {
        ScanResult result = dynamoDB.scan(createScanRequestByTable(tableName));
        return DynamoDBCompactionJobStatusFormat.streamJobStatuses(result.getItems())
                .filter(job -> job.getTaskId().equals(taskId))
                .collect(Collectors.toList());
    }

    @Override
    public List<CompactionJobStatus> getUnfinishedJobs(String tableName) {
        ScanResult result = dynamoDB.scan(createScanRequestByTable(tableName));
        return DynamoDBCompactionJobStatusFormat.streamJobStatuses(result.getItems())
                .filter(job -> !job.isFinished())
                .collect(Collectors.toList());
    }

    @Override
    public List<CompactionJobStatus> getJobsInTimePeriod(String tableName, Instant startTime, Instant endTime) {
        ScanResult result = dynamoDB.scan(createScanRequestByTable(tableName));
        return DynamoDBCompactionJobStatusFormat.streamJobStatuses(result.getItems())
                .filter(job -> job.isInPeriod(startTime, endTime))
                .collect(Collectors.toList());
    }

    private ScanRequest createScanRequestByTable(String tableName) {
        return new ScanRequest()
                .withTableName(statusTableName)
                .addScanFilterEntry(TABLE_NAME, new Condition()
                        .withAttributeValueList(createStringAttribute(tableName))
                        .withComparisonOperator(ComparisonOperator.EQ));
    }

    @Override
    public void setTimeToLive(Long timeToLive) {
        this.timeToLive = timeToLive;
    }

    @Override
    public Long getTimeToLive() {
        return timeToLive;
    }
}
