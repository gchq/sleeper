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
package sleeper.compaction.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static sleeper.compaction.dynamodb.DynamoDBCompactionJobEventFormat.EVENT_TIME;
import static sleeper.compaction.dynamodb.DynamoDBCompactionJobEventFormat.JOB_ID;
import static sleeper.compaction.dynamodb.DynamoDBCompactionJobEventStore.jobEventsTableName;

public class DynamoDBCompactionJobEventStoreCreator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBCompactionJobEventStoreCreator.class);

    private final String instanceId;
    private final AmazonDynamoDB dynamoDB;

    public DynamoDBCompactionJobEventStoreCreator(String instanceId, AmazonDynamoDB dynamoDB) {
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId must not be null");
        this.dynamoDB = Objects.requireNonNull(dynamoDB, "dynamoDB must not be null");
    }

    public void create() {
        initialiseTable(jobEventsTableName(instanceId),
                Arrays.asList(
                        new AttributeDefinition(JOB_ID, ScalarAttributeType.S),
                        new AttributeDefinition(EVENT_TIME, ScalarAttributeType.N)),
                Arrays.asList(
                        new KeySchemaElement(JOB_ID, KeyType.HASH),
                        new KeySchemaElement(EVENT_TIME, KeyType.RANGE)));
    }

    private void initialiseTable(
            String tableName,
            List<AttributeDefinition> attributeDefinitions,
            List<KeySchemaElement> keySchemaElements) {

        CreateTableRequest request = new CreateTableRequest()
                .withTableName(tableName)
                .withAttributeDefinitions(attributeDefinitions)
                .withKeySchema(keySchemaElements)
                .withBillingMode(BillingMode.PAY_PER_REQUEST);
        try {
            CreateTableResult result = dynamoDB.createTable(request);
            LOGGER.info("Created table {}", result.getTableDescription().getTableName());
        } catch (ResourceInUseException e) {
            if (e.getMessage().contains("Table already exists")) {
                LOGGER.warn("Table {} already exists", tableName);
            } else {
                throw e;
            }
        }
    }
}
