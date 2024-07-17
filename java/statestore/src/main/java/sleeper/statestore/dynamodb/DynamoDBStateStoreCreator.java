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
package sleeper.statestore.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.dynamodb.tools.DynamoDBUtils;

import java.util.List;
import java.util.Objects;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.ACTIVE_FILES_TABLELENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.FILE_REFERENCE_COUNT_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_TABLENAME;
import static sleeper.statestore.dynamodb.DynamoDBStateStore.FILE_NAME;
import static sleeper.statestore.dynamodb.DynamoDBStateStore.PARTITION_ID;
import static sleeper.statestore.dynamodb.DynamoDBStateStore.PARTITION_ID_AND_FILENAME;
import static sleeper.statestore.dynamodb.DynamoDBStateStore.TABLE_ID;

/**
 * Creates the tables necessary for a DynamoDB state store. Mainly used for testing purposes as the creation of the
 * tables in real deployments is normally done using CDK.
 */
public class DynamoDBStateStoreCreator {
    private final InstanceProperties instanceProperties;
    private final AmazonDynamoDB dynamoDB;

    public DynamoDBStateStoreCreator(InstanceProperties instanceProperties, AmazonDynamoDB dynamoDB) {
        this.instanceProperties = Objects.requireNonNull(instanceProperties, "instanceProperties must not be null");
        this.dynamoDB = Objects.requireNonNull(dynamoDB, "dynamoDB must not be null");
    }

    /**
     * Creates the DynamoDB tables.
     */
    public void create() {
        createFileReferenceTables();
        createPartitionInfoTable();
    }

    private void createFileReferenceTables() {
        List<AttributeDefinition> activeFilesAttributeDefinitions = List.of(
                new AttributeDefinition(TABLE_ID, ScalarAttributeType.S),
                new AttributeDefinition(PARTITION_ID_AND_FILENAME, ScalarAttributeType.S));
        List<KeySchemaElement> activeFilesKeySchemaElements = List.of(
                new KeySchemaElement(TABLE_ID, KeyType.HASH),
                new KeySchemaElement(PARTITION_ID_AND_FILENAME, KeyType.RANGE));
        initialiseTable(instanceProperties.get(ACTIVE_FILES_TABLELENAME), activeFilesAttributeDefinitions, activeFilesKeySchemaElements);
        List<AttributeDefinition> fileReferenceCountAttributeDefinitions = List.of(
                new AttributeDefinition(TABLE_ID, ScalarAttributeType.S),
                new AttributeDefinition(FILE_NAME, ScalarAttributeType.S));
        List<KeySchemaElement> fileReferenceCountKeySchemaElements = List.of(
                new KeySchemaElement(TABLE_ID, KeyType.HASH),
                new KeySchemaElement(FILE_NAME, KeyType.RANGE));
        initialiseTable(instanceProperties.get(FILE_REFERENCE_COUNT_TABLENAME),
                fileReferenceCountAttributeDefinitions, fileReferenceCountKeySchemaElements);
    }

    private void createPartitionInfoTable() {
        List<AttributeDefinition> attributeDefinitions = List.of(
                new AttributeDefinition(TABLE_ID, ScalarAttributeType.S),
                new AttributeDefinition(PARTITION_ID, ScalarAttributeType.S));
        List<KeySchemaElement> keySchemaElements = List.of(
                new KeySchemaElement(TABLE_ID, KeyType.HASH),
                new KeySchemaElement(PARTITION_ID, KeyType.RANGE));
        initialiseTable(instanceProperties.get(PARTITION_TABLENAME), attributeDefinitions, keySchemaElements);
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
