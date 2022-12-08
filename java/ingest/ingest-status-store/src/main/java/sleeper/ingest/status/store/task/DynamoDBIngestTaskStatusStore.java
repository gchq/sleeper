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

package sleeper.ingest.status.store.task;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.ingest.IngestStatusStoreException;
import sleeper.ingest.task.IngestTaskStatus;
import sleeper.ingest.task.IngestTaskStatusStore;

import java.util.Map;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_STATUS_STORE_ENABLED;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.instanceTableName;
import static sleeper.ingest.status.store.task.DynamoDBIngestTaskStatusFormat.TASK_ID;

public class DynamoDBIngestTaskStatusStore implements IngestTaskStatusStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBIngestTaskStatusStore.class);
    private final AmazonDynamoDB dynamoDB;
    private final String statusTableName;
    private final long timeToLive;

    private DynamoDBIngestTaskStatusStore(AmazonDynamoDB dynamoDB, InstanceProperties properties) {
        this.dynamoDB = dynamoDB;
        this.statusTableName = taskStatusTableName(properties.get(ID));
        this.timeToLive = properties.getLong(UserDefinedInstanceProperty.INGEST_TASK_STATUS_TTL_IN_SECONDS) * 1000;
    }

    public static IngestTaskStatusStore from(AmazonDynamoDB dynamoDB, InstanceProperties properties) {
        if (Boolean.TRUE.equals(properties.getBoolean(INGEST_STATUS_STORE_ENABLED))) {
            return new DynamoDBIngestTaskStatusStore(dynamoDB, properties);
        } else {
            return IngestTaskStatusStore.none();
        }
    }

    public static String taskStatusTableName(String instanceId) {
        return instanceTableName(instanceId, "ingest-task-status");
    }

    @Override
    public void taskStarted(IngestTaskStatus taskStatus) {
        try {
            PutItemResult result = putItem(DynamoDBIngestTaskStatusFormat.createTaskStartedRecord(taskStatus, timeToLive));
            LOGGER.debug("Put started event for task {} to table {}, capacity consumed = {}",
                    taskStatus.getTaskId(), statusTableName, result.getConsumedCapacity().getCapacityUnits());
        } catch (RuntimeException e) {
            throw new IngestStatusStoreException("Failed putItem in taskStarted", e);
        }
    }

    @Override
    public void taskFinished(IngestTaskStatus taskStatus) {
        try {
            PutItemResult result = putItem(DynamoDBIngestTaskStatusFormat.createTaskFinishedRecord(taskStatus, timeToLive));
            LOGGER.debug("Put finished event for task {} to table {}, capacity consumed = {}",
                    taskStatus.getTaskId(), statusTableName, result.getConsumedCapacity().getCapacityUnits());
        } catch (RuntimeException e) {
            throw new IngestStatusStoreException("Failed putItem in taskFinished", e);
        }
    }

    @Override
    public IngestTaskStatus getTask(String taskId) {
        QueryResult result = dynamoDB.query(new QueryRequest()
                .withTableName(statusTableName)
                .addKeyConditionsEntry(TASK_ID, new Condition()
                        .withAttributeValueList(createStringAttribute(taskId))
                        .withComparisonOperator(ComparisonOperator.EQ)));
        return DynamoDBIngestTaskStatusFormat.streamTaskStatuses(result.getItems())
                .findFirst().orElse(null);
    }

    private PutItemResult putItem(Map<String, AttributeValue> item) {
        PutItemRequest putItemRequest = new PutItemRequest()
                .withItem(item)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withTableName(statusTableName);
        return dynamoDB.putItem(putItemRequest);
    }
}
