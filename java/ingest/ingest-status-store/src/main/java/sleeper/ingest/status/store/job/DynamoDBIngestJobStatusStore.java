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

package sleeper.ingest.status.store.job;

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
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.ingest.IngestStatusStoreException;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.status.IngestJobStatus;
import sleeper.ingest.job.status.IngestJobStatusStore;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_STATUS_STORE_ENABLED;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.instanceTableName;

public class DynamoDBIngestJobStatusStore implements IngestJobStatusStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBIngestJobStatusStore.class);

    private final AmazonDynamoDB dynamoDB;
    private final String statusTableName;
    private final DynamoDBIngestJobStatusFormat format;

    private DynamoDBIngestJobStatusStore(AmazonDynamoDB dynamoDB, InstanceProperties properties) {
        this(dynamoDB, properties, Instant::now);
    }

    public DynamoDBIngestJobStatusStore(AmazonDynamoDB dynamoDB, InstanceProperties properties, Supplier<Instant> getTimeNow) {
        this.dynamoDB = dynamoDB;
        this.statusTableName = jobStatusTableName(properties.get(ID));
        long timeToLiveInSeconds = properties.getLong(UserDefinedInstanceProperty.INGEST_JOB_STATUS_TTL_IN_SECONDS);
        this.format = new DynamoDBIngestJobStatusFormat(timeToLiveInSeconds, getTimeNow);
    }

    public static IngestJobStatusStore from(AmazonDynamoDB dynamoDB, InstanceProperties properties) {
        if (properties.getBoolean(INGEST_STATUS_STORE_ENABLED)) {
            return new DynamoDBIngestJobStatusStore(dynamoDB, properties);
        } else {
            return IngestJobStatusStore.none();
        }
    }

    public static String jobStatusTableName(String instanceId) {
        return instanceTableName(instanceId, "ingest-job-status");
    }

    @Override
    public void jobStarted(String taskId, IngestJob job, Instant startTime) {
        try {
            PutItemResult result = putItem(format.createJobStartedRecord(job, startTime, taskId));
            LOGGER.debug("Put started event for job {} to table {}, capacity consumed = {}",
                    job.getId(), statusTableName, result.getConsumedCapacity().getCapacityUnits());
        } catch (RuntimeException e) {
            throw new IngestStatusStoreException("Failed putItem in jobStarted", e);
        }
    }

    @Override
    public void jobFinished(String taskId, IngestJob job, RecordsProcessedSummary summary) {
        try {
            PutItemResult result = putItem(format.createJobFinishedRecord(job, summary, taskId));
            LOGGER.debug("Put finished event for job {} to table {}, capacity consumed = {}",
                    job.getId(), statusTableName, result.getConsumedCapacity().getCapacityUnits());
        } catch (RuntimeException e) {
            throw new IngestStatusStoreException("Failed putItem in jobFinished", e);
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
    public Optional<IngestJobStatus> getJob(String jobId) {
        return getJobStream(jobId).findFirst();
    }

    private Stream<IngestJobStatus> getJobStream(String jobId) {
        QueryResult result = dynamoDB.query(new QueryRequest()
                .withTableName(statusTableName)
                .addKeyConditionsEntry(DynamoDBIngestJobStatusFormat.JOB_ID, new Condition()
                        .withAttributeValueList(createStringAttribute(jobId))
                        .withComparisonOperator(ComparisonOperator.EQ)));
        return DynamoDBIngestJobStatusFormat.streamJobStatuses(result.getItems());
    }

    @Override
    public List<IngestJobStatus> getJobsByTaskId(String tableName, String taskId) {
        ScanResult result = dynamoDB.scan(createScanRequestByTable(tableName));
        return DynamoDBIngestJobStatusFormat.streamJobStatuses(result.getItems())
                .filter(job -> job.isTaskIdAssigned(taskId))
                .collect(Collectors.toList());
    }

    @Override
    public List<IngestJobStatus> getUnfinishedJobs(String tableName) {
        ScanResult result = dynamoDB.scan(createScanRequestByTable(tableName));
        return DynamoDBIngestJobStatusFormat.streamJobStatuses(result.getItems())
                .filter(job -> !job.isFinished())
                .collect(Collectors.toList());
    }

    @Override
    public List<IngestJobStatus> getAllJobs(String tableName) {
        ScanResult result = dynamoDB.scan(createScanRequestByTable(tableName));
        return DynamoDBIngestJobStatusFormat.streamJobStatuses(result.getItems())
                .collect(Collectors.toList());
    }

    @Override
    public List<IngestJobStatus> getJobsInTimePeriod(String tableName, Instant startTime, Instant endTime) {
        ScanResult result = dynamoDB.scan(createScanRequestByTable(tableName));
        return DynamoDBIngestJobStatusFormat.streamJobStatuses(result.getItems())
                .filter(job -> job.isInPeriod(startTime, endTime))
                .collect(Collectors.toList());
    }

    private ScanRequest createScanRequestByTable(String tableName) {
        return new ScanRequest()
                .withTableName(statusTableName)
                .addScanFilterEntry(DynamoDBIngestJobStatusFormat.TABLE_NAME, new Condition()
                        .withAttributeValueList(createStringAttribute(tableName))
                        .withComparisonOperator(ComparisonOperator.EQ));
    }

}
