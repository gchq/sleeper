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

package sleeper.ingest.status.store.task;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.Arrays;

import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.dynamodb.tools.DynamoDBUtils.configureTimeToLive;
import static sleeper.dynamodb.tools.DynamoDBUtils.initialiseTable;
import static sleeper.ingest.status.store.task.DynamoDBIngestTaskStatusFormat.EXPIRY_DATE;
import static sleeper.ingest.status.store.task.DynamoDBIngestTaskStatusFormat.TASK_ID;
import static sleeper.ingest.status.store.task.DynamoDBIngestTaskStatusFormat.UPDATE_TIME;

public class DynamoDBIngestTaskStatusStoreCreator {
    private DynamoDBIngestTaskStatusStoreCreator() {
    }

    public static void create(InstanceProperties properties, AmazonDynamoDB dynamoDB) {
        String tableName = DynamoDBIngestTaskStatusStore.taskStatusTableName(properties.get(ID));
        initialiseTable(dynamoDB, tableName,
                Arrays.asList(
                        new AttributeDefinition(TASK_ID, ScalarAttributeType.S),
                        new AttributeDefinition(UPDATE_TIME, ScalarAttributeType.N)),
                Arrays.asList(
                        new KeySchemaElement(TASK_ID, KeyType.HASH),
                        new KeySchemaElement(UPDATE_TIME, KeyType.RANGE)));
        configureTimeToLive(dynamoDB, tableName, EXPIRY_DATE);
    }
}
