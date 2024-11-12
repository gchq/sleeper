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
package sleeper.compaction.status.store.task;

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

import sleeper.compaction.core.task.CompactionTaskStatus;
import sleeper.compaction.core.task.CompactionTaskStatusStore;
import sleeper.compaction.status.store.CompactionStatusStoreException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.LoggedDuration;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static sleeper.compaction.status.store.task.DynamoDBCompactionTaskStatusFormat.TASK_ID;
import static sleeper.compaction.status.store.task.DynamoDBCompactionTaskStatusFormat.UPDATE_TYPE;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_STATUS_TTL_IN_SECONDS;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.instanceTableName;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;

public class DynamoDBCompactionTaskStatusStore implements CompactionTaskStatusStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBCompactionTaskStatusStore.class);
    private final AmazonDynamoDB dynamoDB;
    private final String statusTableName;
    private final DynamoDBCompactionTaskStatusFormat format;

    public DynamoDBCompactionTaskStatusStore(AmazonDynamoDB dynamoDB, InstanceProperties properties) {
        this(dynamoDB, properties, Instant::now);
    }

    public DynamoDBCompactionTaskStatusStore(
            AmazonDynamoDB dynamoDB, InstanceProperties properties, Supplier<Instant> getTimeNow) {
        this.dynamoDB = dynamoDB;
        this.statusTableName = taskStatusTableName(properties.get(ID));
        int timeToLiveInSeconds = properties.getInt(COMPACTION_TASK_STATUS_TTL_IN_SECONDS);
        format = new DynamoDBCompactionTaskStatusFormat(timeToLiveInSeconds, getTimeNow);
    }

    @Override
    public void taskStarted(CompactionTaskStatus taskStatus) {
        try {
            putItem(format.createTaskStartedRecord(taskStatus));
        } catch (RuntimeException e) {
            throw new CompactionStatusStoreException("Failed putItem in taskStarted for task " + taskStatus.getTaskId(), e);
        }
    }

    @Override
    public void taskFinished(CompactionTaskStatus taskStatus) {
        try {
            putItem(format.createTaskFinishedRecord(taskStatus));
        } catch (RuntimeException e) {
            throw new CompactionStatusStoreException("Failed putItem in taskFinished for task " + taskStatus.getTaskId(), e);
        }
    }

    @Override
    public CompactionTaskStatus getTask(String taskId) {
        QueryResult result = dynamoDB.query(new QueryRequest()
                .withTableName(statusTableName)
                .addKeyConditionsEntry(TASK_ID, new Condition()
                        .withAttributeValueList(createStringAttribute(taskId))
                        .withComparisonOperator(ComparisonOperator.EQ)));
        return DynamoDBCompactionTaskStatusFormat.streamTaskStatuses(result.getItems().stream())
                .findFirst().orElse(null);
    }

    @Override
    public List<CompactionTaskStatus> getAllTasks() {
        return DynamoDBCompactionTaskStatusFormat.streamTaskStatuses(
                streamPagedItems(dynamoDB, new ScanRequest().withTableName(statusTableName)))
                .collect(Collectors.toList());
    }

    @Override
    public List<CompactionTaskStatus> getTasksInTimePeriod(Instant startTime, Instant endTime) {
        return DynamoDBCompactionTaskStatusFormat.streamTaskStatuses(
                streamPagedItems(dynamoDB, new ScanRequest().withTableName(statusTableName)))
                .filter(task -> task.isInPeriod(startTime, endTime))
                .collect(Collectors.toList());
    }

    @Override
    public List<CompactionTaskStatus> getTasksInProgress() {
        return DynamoDBCompactionTaskStatusFormat.streamTaskStatuses(
                streamPagedItems(dynamoDB, new ScanRequest().withTableName(statusTableName)))
                .filter(task -> !task.isFinished())
                .collect(Collectors.toList());
    }

    private void putItem(Map<String, AttributeValue> item) {
        Instant startTime = Instant.now();
        PutItemRequest putItemRequest = new PutItemRequest()
                .withItem(item)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withTableName(statusTableName);
        PutItemResult result = dynamoDB.putItem(putItemRequest);
        LOGGER.debug("Put {} event for task {} to table {}, capacity consumed = {}, took {}",
                item.get(UPDATE_TYPE).getS(), item.get(TASK_ID).getS(), statusTableName,
                result.getConsumedCapacity().getCapacityUnits(),
                LoggedDuration.withFullOutput(startTime, Instant.now()));
    }

    public static String taskStatusTableName(String instanceId) {
        return instanceTableName(instanceId, "compaction-task-status");
    }
}
