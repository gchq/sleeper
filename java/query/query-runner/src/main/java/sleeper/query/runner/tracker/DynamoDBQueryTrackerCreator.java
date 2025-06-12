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

package sleeper.query.runner.tracker;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_TRACKER_TABLE_NAME;
import static sleeper.query.runner.tracker.DynamoDBQueryTracker.QUERY_ID;
import static sleeper.query.runner.tracker.DynamoDBQueryTracker.SUB_QUERY_ID;

public class DynamoDBQueryTrackerCreator {
    private final InstanceProperties instanceProperties;
    private final DynamoDbClient dynamoClient;

    public DynamoDBQueryTrackerCreator(InstanceProperties instanceProperties, DynamoDbClient dynamoClient) {
        this.instanceProperties = instanceProperties;
        this.dynamoClient = dynamoClient;
    }

    public void create() {
        dynamoClient.createTable(request -> request
                .tableName(instanceProperties.get(QUERY_TRACKER_TABLE_NAME))
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName(QUERY_ID).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(SUB_QUERY_ID).attributeType(ScalarAttributeType.S).build())
                .keySchema(
                        KeySchemaElement.builder().attributeName(QUERY_ID).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(SUB_QUERY_ID).keyType(KeyType.RANGE).build())
                .billingMode(BillingMode.PAY_PER_REQUEST));
    }
}
