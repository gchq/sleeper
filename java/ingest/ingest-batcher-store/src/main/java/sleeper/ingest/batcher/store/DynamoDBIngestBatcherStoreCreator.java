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

package sleeper.ingest.batcher.store;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.waiters.WaiterParameters;

import sleeper.core.properties.instance.InstanceProperties;

import java.util.Arrays;
import java.util.List;

import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.dynamodb.tools.DynamoDBUtils.configureTimeToLive;
import static sleeper.dynamodb.tools.DynamoDBUtils.initialiseTable;
import static sleeper.ingest.batcher.store.DynamoDBIngestRequestFormat.EXPIRY_TIME;
import static sleeper.ingest.batcher.store.DynamoDBIngestRequestFormat.FILE_PATH;
import static sleeper.ingest.batcher.store.DynamoDBIngestRequestFormat.JOB_ID;

public class DynamoDBIngestBatcherStoreCreator {
    private DynamoDBIngestBatcherStoreCreator() {
    }

    public static void create(InstanceProperties properties, AmazonDynamoDB dynamoDB) {
        String tableName = DynamoDBIngestBatcherStore.ingestRequestsTableName(properties.get(ID));
        initialiseTable(dynamoDB, tableName,
                List.of(
                        new AttributeDefinition(JOB_ID, ScalarAttributeType.S),
                        new AttributeDefinition(FILE_PATH, ScalarAttributeType.S)),
                Arrays.asList(
                        new KeySchemaElement(JOB_ID, KeyType.HASH),
                        new KeySchemaElement(FILE_PATH, KeyType.RANGE)));
        configureTimeToLive(dynamoDB, tableName, EXPIRY_TIME);
    }

    public static void tearDown(InstanceProperties properties, AmazonDynamoDB dynamoDB) {
        String tableName = DynamoDBIngestBatcherStore.ingestRequestsTableName(properties.get(ID));
        dynamoDB.deleteTable(tableName);
        dynamoDB.waiters().tableNotExists().run(new WaiterParameters<>(
                new DescribeTableRequest(tableName)));
    }
}
