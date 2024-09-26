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
package sleeper.statestore.transactionlog;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import sleeper.core.properties.instance.InstanceProperties;

import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME;

/**
 * Creates the DynamoDB tables needed to store an index of snapshots derived from a transaction log. Mainly used for
 * testing purposes as the creation of the tables in real deployments is normally done using CDK.
 */
public class DynamoDBTransactionLogSnapshotMetadataStoreCreator {
    private final AmazonDynamoDB dynamoDB;
    private final InstanceProperties instanceProperties;

    public DynamoDBTransactionLogSnapshotMetadataStoreCreator(InstanceProperties instanceProperties, AmazonDynamoDB dynamoDB) {
        this.dynamoDB = dynamoDB;
        this.instanceProperties = instanceProperties;
    }

    /**
     * Creates the needed DynamoDB tables.
     */
    public void create() {
        createLatestSnapshotTable();
        createAllSnapshotsTable();
    }

    private void createLatestSnapshotTable() {
        List<AttributeDefinition> attributeDefinitions = List.of(
                new AttributeDefinition(DynamoDBTransactionLogSnapshotMetadataStore.TABLE_ID, ScalarAttributeType.S));
        List<KeySchemaElement> keySchemaElements = List.of(
                new KeySchemaElement(DynamoDBTransactionLogSnapshotMetadataStore.TABLE_ID, KeyType.HASH));
        CreateTableRequest request = new CreateTableRequest()
                .withTableName(instanceProperties.get(TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME))
                .withAttributeDefinitions(attributeDefinitions)
                .withKeySchema(keySchemaElements)
                .withBillingMode(BillingMode.PAY_PER_REQUEST);
        dynamoDB.createTable(request);
    }

    private void createAllSnapshotsTable() {
        List<AttributeDefinition> attributeDefinitions = List.of(
                new AttributeDefinition(DynamoDBTransactionLogSnapshotMetadataStore.TABLE_ID_AND_SNAPSHOT_TYPE, ScalarAttributeType.S),
                new AttributeDefinition(DynamoDBTransactionLogSnapshotMetadataStore.TRANSACTION_NUMBER, ScalarAttributeType.N));
        List<KeySchemaElement> keySchemaElements = List.of(
                new KeySchemaElement(DynamoDBTransactionLogSnapshotMetadataStore.TABLE_ID_AND_SNAPSHOT_TYPE, KeyType.HASH),
                new KeySchemaElement(DynamoDBTransactionLogSnapshotMetadataStore.TRANSACTION_NUMBER, KeyType.RANGE));
        CreateTableRequest request = new CreateTableRequest()
                .withTableName(instanceProperties.get(TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME))
                .withAttributeDefinitions(attributeDefinitions)
                .withKeySchema(keySchemaElements)
                .withBillingMode(BillingMode.PAY_PER_REQUEST);
        dynamoDB.createTable(request);
    }
}
