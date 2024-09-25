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

package sleeper.configuration.table.index;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.dynamodb.tools.DynamoDBUtils;

import java.util.List;
import java.util.Objects;

import static sleeper.configuration.table.index.DynamoDBTableIndex.TABLE_ID_FIELD;
import static sleeper.configuration.table.index.DynamoDBTableIndex.TABLE_NAME_FIELD;
import static sleeper.configuration.table.index.DynamoDBTableIndex.TABLE_ONLINE_FIELD;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_ID_INDEX_DYNAMO_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_NAME_INDEX_DYNAMO_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_ONLINE_INDEX_DYNAMO_TABLENAME;

/**
 * Creates the DynamoDB tables required to implement a Sleeper table index. Usually this will be done by the CDK. This
 * is used for a local instance or integration tests.
 */
public class DynamoDBTableIndexCreator {
    private final InstanceProperties instanceProperties;
    private final AmazonDynamoDB dynamoDB;

    private DynamoDBTableIndexCreator(InstanceProperties instanceProperties, AmazonDynamoDB dynamoDB) {
        this.instanceProperties = Objects.requireNonNull(instanceProperties, "instanceProperties must not be null");
        this.dynamoDB = Objects.requireNonNull(dynamoDB, "dynamoDB must not be null");
    }

    /**
     * Creates the DynamoDB tables for a Sleeper table index.
     *
     * @param dynamoDBClient     the DynamoDB client
     * @param instanceProperties the instance properties
     */
    public static void create(AmazonDynamoDB dynamoDBClient, InstanceProperties instanceProperties) {
        new DynamoDBTableIndexCreator(instanceProperties, dynamoDBClient).create();
    }

    private void create() {
        initialiseTable(instanceProperties.get(TABLE_NAME_INDEX_DYNAMO_TABLENAME),
                List.of(new AttributeDefinition(TABLE_NAME_FIELD, ScalarAttributeType.S)),
                List.of(new KeySchemaElement(TABLE_NAME_FIELD, KeyType.HASH)));
        initialiseTable(instanceProperties.get(TABLE_ID_INDEX_DYNAMO_TABLENAME),
                List.of(new AttributeDefinition(TABLE_ID_FIELD, ScalarAttributeType.S)),
                List.of(new KeySchemaElement(TABLE_ID_FIELD, KeyType.HASH)));
        initialiseTable(instanceProperties.get(TABLE_ONLINE_INDEX_DYNAMO_TABLENAME),
                List.of(new AttributeDefinition(TABLE_ONLINE_FIELD, ScalarAttributeType.S),
                        new AttributeDefinition(TABLE_NAME_FIELD, ScalarAttributeType.S)),
                List.of(new KeySchemaElement(TABLE_ONLINE_FIELD, KeyType.HASH),
                        new KeySchemaElement(TABLE_NAME_FIELD, KeyType.RANGE)));
    }

    private void initialiseTable(
            String tableName,
            List<AttributeDefinition> attributeDefinitions,
            List<KeySchemaElement> keySchemaElements) {
        DynamoDBUtils.initialiseTable(dynamoDB,
                tableName, attributeDefinitions, keySchemaElements,
                instanceProperties.getTags());
    }
}
