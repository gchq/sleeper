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
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.CompactionJobSummary;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.status.CompactionStatusStoreException;
import sleeper.configuration.properties.InstanceProperties;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static sleeper.compaction.status.DynamoDBAttributes.createStringAttribute;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.JOB_ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_STATUS_STORE_ENABLED;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;

public class DynamoDBCompactionJobStatusStore implements CompactionJobStatusStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBCompactionJobStatusStore.class);

    private final AmazonDynamoDB dynamoDB;
    private final String statusTableName;

    private DynamoDBCompactionJobStatusStore(AmazonDynamoDB dynamoDB, InstanceProperties properties) {
        this.dynamoDB = dynamoDB;
        this.statusTableName = jobStatusTableName(properties.get(ID));
    }

    public static CompactionJobStatusStore from(AmazonDynamoDB dynamoDB, InstanceProperties properties) {
        if (Boolean.TRUE.equals(properties.getBoolean(COMPACTION_STATUS_STORE_ENABLED))) {
            return new DynamoDBCompactionJobStatusStore(dynamoDB, properties);
        } else {
            return CompactionJobStatusStore.none();
        }
    }

    @Override
    public void jobCreated(CompactionJob job) {
        try {
            PutItemResult result = putItem(DynamoDBCompactionJobStatusFormat.createJobCreatedRecord(job));
            LOGGER.debug("Put created event for job {} to table {}, capacity consumed = {}",
                    job.getId(), statusTableName, result.getConsumedCapacity().getCapacityUnits());
        } catch (RuntimeException e) {
            throw new CompactionStatusStoreException("Failed putItem in jobCreated", e);
        }
    }

    @Override
    public void jobStarted(CompactionJob job, Instant startTime) {
        try {
            PutItemResult result = putItem(DynamoDBCompactionJobStatusFormat.createJobStartedRecord(job, startTime));
            LOGGER.debug("Put started event for job {} to table {}, capacity consumed = {}",
                    job.getId(), statusTableName, result.getConsumedCapacity().getCapacityUnits());
        } catch (RuntimeException e) {
            throw new CompactionStatusStoreException("Failed putItem in jobStarted", e);
        }
    }

    @Override
    public void jobFinished(CompactionJob job, CompactionJobSummary summary) {
        try {
            PutItemResult result = putItem(DynamoDBCompactionJobStatusFormat.createJobFinishedRecord(job, summary));
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
        List<CompactionJobStatus> statuses = DynamoDBCompactionJobStatusFormat.getJobStatuses(result.getItems());
        if (statuses.isEmpty()) {
            return null;
        }
        return statuses.get(0);
    }

    @Override
    public List<CompactionJobStatus> getUnfinishedJobs(String tableName) {
        return getAllJobs();
    }

    @Override
    public List<CompactionJobStatus> getJobsInTimePeriod(String tableName, Instant startTime, Instant endTime) {
        return getAllJobs();
    }

    private List<CompactionJobStatus> getAllJobs() {
        ScanResult result = dynamoDB.scan(statusTableName, Collections.emptyMap());
        return DynamoDBCompactionJobStatusFormat.getJobStatuses(result.getItems());
    }

    public static String jobStatusTableName(String instanceId) {
        return instanceTableName(instanceId, "compaction-job-status");
    }

    private static String instanceTableName(String instanceId, String tableName) {
        return String.join("-", "sleeper", instanceId, tableName);
    }
}
