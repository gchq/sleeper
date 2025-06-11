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
package sleeper.statestore.transactionlog;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotMetadataStoreCreator;

import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_FILES_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_PARTITIONS_TABLENAME;

/**
 * Creates the DynamoDB tables needed for a state store derived from a transaction log. Mainly used for testing purposes
 * as the creation of the tables in real deployments is normally done using CDK.
 */
public class TransactionLogStateStoreCreator {
    private final DynamoDbClient dynamoDB;
    private final InstanceProperties instanceProperties;

    public TransactionLogStateStoreCreator(InstanceProperties instanceProperties, DynamoDbClient dynamoDB) {
        this.dynamoDB = dynamoDB;
        this.instanceProperties = instanceProperties;
    }

    /**
     * Creates the needed DynamoDB tables.
     */
    public void create() {
        new DynamoDBTransactionLogSnapshotMetadataStoreCreator(instanceProperties, dynamoDB).create();
        createTransactionLogTable(instanceProperties.get(TRANSACTION_LOG_FILES_TABLENAME));
        createTransactionLogTable(instanceProperties.get(TRANSACTION_LOG_PARTITIONS_TABLENAME));
    }

    private void createTransactionLogTable(String tableName) {
        List<AttributeDefinition> attributeDefinitions = List.of(
                AttributeDefinition.builder()
                        .attributeName(DynamoDBTransactionLogStateStore.TABLE_ID)
                        .attributeType(ScalarAttributeType.S)
                        .build(),
                AttributeDefinition.builder()
                        .attributeName(DynamoDBTransactionLogStateStore.TRANSACTION_NUMBER)
                        .attributeType(ScalarAttributeType.N)
                        .build());
        List<KeySchemaElement> keySchemaElements = List.of(
                KeySchemaElement.builder()
                        .attributeName(DynamoDBTransactionLogStateStore.TABLE_ID)
                        .keyType(KeyType.HASH)
                        .build(),
                KeySchemaElement.builder()
                        .attributeName(DynamoDBTransactionLogStateStore.TRANSACTION_NUMBER)
                        .keyType(KeyType.RANGE)
                        .build());
        CreateTableRequest request = CreateTableRequest.builder()
                .tableName(tableName)
                .attributeDefinitions(attributeDefinitions)
                .keySchema(keySchemaElements)
                .billingMode(BillingMode.PAY_PER_REQUEST).build();
        dynamoDB.createTable(request);
    }
}
