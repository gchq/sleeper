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

package sleeper.ingest.batcher.store;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

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

    public static void create(InstanceProperties properties, DynamoDbClient dynamoDB) {
        String tableName = DynamoDBIngestBatcherStore.ingestRequestsTableName(properties.get(ID));
        initialiseTable(dynamoDB, tableName,
                List.of(
                        AttributeDefinition.builder().attributeName(JOB_ID).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(FILE_PATH).attributeType(ScalarAttributeType.S).build()),
                Arrays.asList(
                        KeySchemaElement.builder().attributeName(JOB_ID).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(FILE_PATH).keyType(KeyType.RANGE).build()));
        configureTimeToLive(dynamoDB, tableName, EXPIRY_TIME);
    }
}
