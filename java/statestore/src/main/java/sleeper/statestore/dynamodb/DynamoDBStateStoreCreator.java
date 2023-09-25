/*
 * Copyright 2022-2023 Crown Copyright
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
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.statestore.dynamodb.DynamoDBStateStore.FILE_NAME;
import static sleeper.statestore.dynamodb.DynamoDBStateStore.PARTITION_ID;

/**
 * Creates the tables necessary for a {@link DynamoDBStateStore}. Mainly used
 * for testing purposes as the creation of the tables in real deployments is
 * normally done using CDK.
 */
public class DynamoDBStateStoreCreator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBStateStoreCreator.class);
    private final AmazonDynamoDB dynamoDB;
    private final InstanceProperties instanceProperties;
    private final Collection<Tag> tags;

    public DynamoDBStateStoreCreator(InstanceProperties instanceProperties, AmazonDynamoDB dynamoDB) {
        this.dynamoDB = Objects.requireNonNull(dynamoDB, "dynamoDB must not be null");
        this.instanceProperties = instanceProperties;
        this.tags = instanceProperties.getTags()
                .entrySet()
                .stream()
                .map(e -> new Tag().withKey(e.getKey()).withValue(e.getValue()))
                .collect(Collectors.toList());
    }

    public void create() {
        createFileInfoTables();
        createPartitionInfoTable();
    }

    public DynamoDBStateStore create(TableProperties tableProperties) {
        createFileInfoTables(tableProperties);
        createPartitionInfoTable(tableProperties);
        return new DynamoDBStateStore(tableProperties, dynamoDB);
    }

    public void createFileInfoTables() {
    }

    public void createFileInfoTables(TableProperties tableProperties) {
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition(FILE_NAME, ScalarAttributeType.S));
        List<KeySchemaElement> keySchemaElements = new ArrayList<>();
        keySchemaElements.add(new KeySchemaElement(FILE_NAME, KeyType.HASH));
        initialiseTable(tableProperties.get(ACTIVE_FILEINFO_TABLENAME), attributeDefinitions, keySchemaElements);
        initialiseTable(tableProperties.get(READY_FOR_GC_FILEINFO_TABLENAME), attributeDefinitions, keySchemaElements);
    }

    public void createPartitionInfoTable() {
    }

    public void createPartitionInfoTable(TableProperties tableProperties) {
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition(PARTITION_ID, ScalarAttributeType.S));
        List<KeySchemaElement> keySchemaElements = new ArrayList<>();
        keySchemaElements.add(new KeySchemaElement(PARTITION_ID, KeyType.HASH));
        initialiseTable(tableProperties.get(PARTITION_TABLENAME), attributeDefinitions, keySchemaElements);
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
        String message = "";
        if (!tags.isEmpty()) {
            request = request.withTags(tags);
            message = " with tags " + tags;
        }
        try {
            CreateTableResult result = dynamoDB.createTable(request);
            LOGGER.info("Created table {} {}", result.getTableDescription().getTableName(), message);
        } catch (ResourceInUseException e) {
            if (e.getMessage().contains("Table already exists")) {
                LOGGER.warn("Table {} already exists", tableName);
            } else {
                throw e;
            }
        }
    }
}
