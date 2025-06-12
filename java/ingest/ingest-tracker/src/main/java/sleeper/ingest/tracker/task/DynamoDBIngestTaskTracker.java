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

package sleeper.ingest.tracker.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.tracker.ingest.task.IngestTaskStatus;
import sleeper.core.tracker.ingest.task.IngestTaskTracker;
import sleeper.core.util.LoggedDuration;
import sleeper.ingest.tracker.IngestTrackerException;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.IngestProperty.INGEST_TASK_STATUS_TTL_IN_SECONDS;
import static sleeper.dynamodb.toolsv2.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.toolsv2.DynamoDBUtils.instanceTableName;
import static sleeper.dynamodb.toolsv2.DynamoDBUtils.streamPagedItems;
import static sleeper.ingest.tracker.task.DynamoDBIngestTaskStatusFormat.TASK_ID;
import static sleeper.ingest.tracker.task.DynamoDBIngestTaskStatusFormat.UPDATE_TYPE;

public class DynamoDBIngestTaskTracker implements IngestTaskTracker {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBIngestTaskTracker.class);
    private final DynamoDbClient dynamoDB;
    private final String statusTableName;
    private final DynamoDBIngestTaskStatusFormat format;

    public DynamoDBIngestTaskTracker(DynamoDbClient dynamoDB, InstanceProperties properties) {
        this(dynamoDB, properties, Instant::now);
    }

    public DynamoDBIngestTaskTracker(
            DynamoDbClient dynamoDB, InstanceProperties properties, Supplier<Instant> getTimeNow) {
        this.dynamoDB = dynamoDB;
        this.statusTableName = taskStatusTableName(properties.get(ID));
        int timeToLiveInSeconds = properties.getInt(INGEST_TASK_STATUS_TTL_IN_SECONDS);
        format = new DynamoDBIngestTaskStatusFormat(timeToLiveInSeconds, getTimeNow);
    }

    public static String taskStatusTableName(String instanceId) {
        return instanceTableName(instanceId, "ingest-task-status");
    }

    @Override
    public void taskStarted(IngestTaskStatus taskStatus) {
        try {
            putItem(format.createTaskStartedRecord(taskStatus));
        } catch (RuntimeException e) {
            throw new IngestTrackerException("Failed putItem in taskStarted for task " + taskStatus.getTaskId(), e);
        }
    }

    @Override
    public void taskFinished(IngestTaskStatus taskStatus) {
        try {
            putItem(format.createTaskFinishedRecord(taskStatus));
        } catch (RuntimeException e) {
            throw new IngestTrackerException("Failed putItem in taskFinished for task " + taskStatus.getTaskId(), e);
        }
    }

    @Override
    public IngestTaskStatus getTask(String taskId) {
        QueryResponse response = dynamoDB.query(QueryRequest.builder()
                .tableName(statusTableName)
                .keyConditions(Map.of(TASK_ID, Condition.builder()
                        .attributeValueList(createStringAttribute(taskId))
                        .comparisonOperator(ComparisonOperator.EQ)
                        .build()))
                .build());
        return DynamoDBIngestTaskStatusFormat.streamTaskStatuses(response.items().stream())
                .findFirst().orElse(null);
    }

    @Override
    public List<IngestTaskStatus> getAllTasks() {
        return DynamoDBIngestTaskStatusFormat.streamTaskStatuses(
                streamPagedItems(dynamoDB, ScanRequest.builder().tableName(statusTableName).build()))
                .collect(Collectors.toList());
    }

    @Override
    public List<IngestTaskStatus> getTasksInTimePeriod(Instant startTime, Instant endTime) {
        return DynamoDBIngestTaskStatusFormat.streamTaskStatuses(
                streamPagedItems(dynamoDB, ScanRequest.builder().tableName(statusTableName).build()))
                .filter(task -> task.isInPeriod(startTime, endTime))
                .collect(Collectors.toList());
    }

    @Override
    public List<IngestTaskStatus> getTasksInProgress() {
        return DynamoDBIngestTaskStatusFormat.streamTaskStatuses(
                streamPagedItems(dynamoDB, ScanRequest.builder().tableName(statusTableName).build()))
                .filter(task -> !task.isFinished())
                .collect(Collectors.toList());
    }

    private void putItem(Map<String, AttributeValue> item) {
        Instant startTime = Instant.now();
        PutItemRequest putItemRequest = PutItemRequest.builder()
                .item(item)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .tableName(statusTableName)
                .build();
        PutItemResponse result = dynamoDB.putItem(putItemRequest);
        LOGGER.debug("Put {} event for task {} to table {}, capacity consumed = {}, took {}",
                item.get(UPDATE_TYPE).s(), item.get(TASK_ID).s(),
                statusTableName, result.consumedCapacity().capacityUnits(),
                LoggedDuration.withFullOutput(startTime, Instant.now()));
    }
}
