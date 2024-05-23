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

package sleeper.statestore.s3;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.List;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.REVISION_TABLENAME;

/**
 * Creates the DynamoDB tables necessary for an S3 state store. Mainly used for testing purposes as the creation of the
 * tables in real deployments is normally done using CDK.
 */
public class S3StateStoreCreator {
    private final AmazonDynamoDB dynamoDB;
    private final InstanceProperties instanceProperties;

    public S3StateStoreCreator(InstanceProperties instanceProperties, AmazonDynamoDB dynamoDB) {
        this.dynamoDB = dynamoDB;
        this.instanceProperties = instanceProperties;
    }

    /**
     * Creates the DynamoDB tables.
     */
    public void create() {
        String tableName = instanceProperties.get(REVISION_TABLENAME);
        List<AttributeDefinition> attributeDefinitions = List.of(
                new AttributeDefinition(S3StateStore.TABLE_ID, ScalarAttributeType.S),
                new AttributeDefinition(S3StateStore.REVISION_ID_KEY, ScalarAttributeType.S));
        List<KeySchemaElement> keySchemaElements = List.of(
                new KeySchemaElement(S3StateStore.TABLE_ID, KeyType.HASH),
                new KeySchemaElement(S3StateStore.REVISION_ID_KEY, KeyType.RANGE));
        CreateTableRequest request = new CreateTableRequest()
                .withTableName(tableName)
                .withAttributeDefinitions(attributeDefinitions)
                .withKeySchema(keySchemaElements)
                .withBillingMode(BillingMode.PAY_PER_REQUEST);
        dynamoDB.createTable(request);
    }
}
