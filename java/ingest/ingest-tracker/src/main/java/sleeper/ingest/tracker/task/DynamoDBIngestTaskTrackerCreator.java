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
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import sleeper.core.properties.instance.InstanceProperties;

import java.util.Arrays;

import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.IngestProperty.INGEST_TRACKER_ENABLED;
import static sleeper.dynamodb.toolsv2.DynamoDBUtils.configureTimeToLive;
import static sleeper.dynamodb.toolsv2.DynamoDBUtils.initialiseTable;
import static sleeper.ingest.tracker.task.DynamoDBIngestTaskStatusFormat.EXPIRY_DATE;
import static sleeper.ingest.tracker.task.DynamoDBIngestTaskStatusFormat.TASK_ID;
import static sleeper.ingest.tracker.task.DynamoDBIngestTaskStatusFormat.UPDATE_TIME;
import static sleeper.ingest.tracker.task.DynamoDBIngestTaskTracker.taskStatusTableName;

public class DynamoDBIngestTaskTrackerCreator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBIngestTaskTrackerCreator.class);

    private DynamoDBIngestTaskTrackerCreator() {
    }

    public static void create(InstanceProperties properties, DynamoDbClient dynamoDB) {
        if (!properties.getBoolean(INGEST_TRACKER_ENABLED)) {
            return;
        }
        String tableName = taskStatusTableName(properties.get(ID));
        initialiseTable(dynamoDB, tableName,
                Arrays.asList(
                        AttributeDefinition.builder().attributeName(TASK_ID).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(UPDATE_TIME).attributeType(ScalarAttributeType.N).build()),
                Arrays.asList(
                        KeySchemaElement.builder().attributeName(TASK_ID).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(UPDATE_TIME).keyType(KeyType.RANGE).build()));
        configureTimeToLive(dynamoDB, tableName, EXPIRY_DATE);
    }

    public static void tearDown(InstanceProperties properties, DynamoDbClient dynamoDBClient) {
        if (!properties.getBoolean(INGEST_TRACKER_ENABLED)) {
            return;
        }
        String tableName = taskStatusTableName(properties.get(ID));
        LOGGER.info("Deleting table: {}", tableName);
        dynamoDBClient.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
    }
}
