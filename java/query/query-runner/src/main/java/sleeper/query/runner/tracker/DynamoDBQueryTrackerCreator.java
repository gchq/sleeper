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

package sleeper.query.runner.tracker;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.collect.Lists;

import sleeper.core.properties.instance.InstanceProperties;

import java.util.Collection;
import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_TRACKER_TABLE_NAME;
import static sleeper.query.runner.tracker.DynamoDBQueryTracker.QUERY_ID;
import static sleeper.query.runner.tracker.DynamoDBQueryTracker.SUB_QUERY_ID;

public class DynamoDBQueryTrackerCreator {
    private final InstanceProperties instanceProperties;
    private final AmazonDynamoDB dynamoDBClient;

    public DynamoDBQueryTrackerCreator(InstanceProperties instanceProperties, AmazonDynamoDB dynamoDBClient) {
        this.instanceProperties = instanceProperties;
        this.dynamoDBClient = dynamoDBClient;
    }

    public void create() {
        String tableName = instanceProperties.get(QUERY_TRACKER_TABLE_NAME);
        dynamoDBClient.createTable(new CreateTableRequest(tableName, createKeySchema())
                .withAttributeDefinitions(createAttributeDefinitions())
                .withBillingMode(BillingMode.PAY_PER_REQUEST));
    }

    private Collection<AttributeDefinition> createAttributeDefinitions() {
        return Lists.newArrayList(
                new AttributeDefinition(QUERY_ID, ScalarAttributeType.S),
                new AttributeDefinition(SUB_QUERY_ID, ScalarAttributeType.S));
    }

    private List<KeySchemaElement> createKeySchema() {
        return Lists.newArrayList(
                new KeySchemaElement()
                        .withAttributeName(QUERY_ID)
                        .withKeyType(KeyType.HASH),
                new KeySchemaElement()
                        .withAttributeName(SUB_QUERY_ID)
                        .withKeyType(KeyType.RANGE));
    }
}
