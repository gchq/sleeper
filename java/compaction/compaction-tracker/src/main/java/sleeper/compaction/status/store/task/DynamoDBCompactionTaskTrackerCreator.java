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
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;

import java.util.Arrays;

import static sleeper.compaction.status.store.task.DynamoDBCompactionTaskStatusFormat.EXPIRY_DATE;
import static sleeper.compaction.status.store.task.DynamoDBCompactionTaskStatusFormat.TASK_ID;
import static sleeper.compaction.status.store.task.DynamoDBCompactionTaskStatusFormat.UPDATE_TIME;
import static sleeper.compaction.status.store.task.DynamoDBCompactionTaskTracker.taskStatusTableName;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TRACKER_ENABLED;
import static sleeper.dynamodb.tools.DynamoDBUtils.configureTimeToLive;
import static sleeper.dynamodb.tools.DynamoDBUtils.initialiseTable;

public class DynamoDBCompactionTaskTrackerCreator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBCompactionTaskTrackerCreator.class);

    private DynamoDBCompactionTaskTrackerCreator() {
    }

    public static void create(InstanceProperties properties, AmazonDynamoDB dynamoDB) {
        if (!properties.getBoolean(COMPACTION_TRACKER_ENABLED)) {
            return;
        }
        String tableName = taskStatusTableName(properties.get(ID));
        initialiseTable(dynamoDB, tableName,
                Arrays.asList(
                        new AttributeDefinition(TASK_ID, ScalarAttributeType.S),
                        new AttributeDefinition(UPDATE_TIME, ScalarAttributeType.N)),
                Arrays.asList(
                        new KeySchemaElement(TASK_ID, KeyType.HASH),
                        new KeySchemaElement(UPDATE_TIME, KeyType.RANGE)));
        configureTimeToLive(dynamoDB, tableName, EXPIRY_DATE);
    }

    public static void tearDown(InstanceProperties properties, AmazonDynamoDB dynamoDBClient) {
        if (!properties.getBoolean(COMPACTION_TRACKER_ENABLED)) {
            return;
        }
        String tableName = taskStatusTableName(properties.get(ID));
        LOGGER.info("Deleting table: {}", tableName);
        dynamoDBClient.deleteTable(tableName);
    }
}
