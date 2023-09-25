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
import sleeper.core.schema.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DYNAMODB_STRONGLY_CONSISTENT_READS;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
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
    private final String activeFileInfoTablename;
    private final String readyForGCFileInfoTablename;
    private final String partitionTableName;
    private final Schema schema;
    private final int garbageCollectorDelayBeforeDeletionInMinutes;
    private final boolean stronglyConsistentReads;
    private final Collection<Tag> tags;

    private DynamoDBStateStoreCreator(
            String activeFileInfoTablename,
            String readyForGCFileInfoTablename,
            String partitionTablename,
            Schema schema,
            int garbageCollectorDelayBeforeDeletionInMinutes,
            boolean stronglyConsistentReads,
            AmazonDynamoDB dynamoDB,
            Map<String, String> tags) {
        this.activeFileInfoTablename = Objects.requireNonNull(activeFileInfoTablename, "activeFileInfoTablename must not be null");
        this.readyForGCFileInfoTablename = Objects.requireNonNull(readyForGCFileInfoTablename, "readyForGCFileInfoTablename must not be null");
        this.partitionTableName = Objects.requireNonNull(partitionTablename, "partitionTableName must not be null");
        this.schema = Objects.requireNonNull(schema, "schema must not be null");
        this.garbageCollectorDelayBeforeDeletionInMinutes = garbageCollectorDelayBeforeDeletionInMinutes;
        this.stronglyConsistentReads = stronglyConsistentReads;
        this.dynamoDB = Objects.requireNonNull(dynamoDB, "dynamoDB must not be null");
        if (null == tags) {
            this.tags = Collections.emptySet();
        } else {
            this.tags = tags
                    .entrySet()
                    .stream()
                    .map(e -> new Tag().withKey(e.getKey()).withValue(e.getValue()))
                    .collect(Collectors.toList());
        }
    }

    public DynamoDBStateStoreCreator(
            InstanceProperties instanceProperties,
            TableProperties tableProperties,
            AmazonDynamoDB dynamoDB) {
        this(tableProperties.get(ACTIVE_FILEINFO_TABLENAME), tableProperties.get(READY_FOR_GC_FILEINFO_TABLENAME),
                tableProperties.get(PARTITION_TABLENAME), tableProperties.getSchema(),
                tableProperties.getInt(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION),
                tableProperties.getBoolean(DYNAMODB_STRONGLY_CONSISTENT_READS),
                dynamoDB, Collections.emptyMap());
    }

    public DynamoDBStateStore create(TableProperties tableProperties) {
        createFileInfoTables();
        createPartitionInfoTable();
        return new DynamoDBStateStore(tableProperties, dynamoDB);
    }

    public void createFileInfoTables() {
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition(FILE_NAME, ScalarAttributeType.S));
        List<KeySchemaElement> keySchemaElements = new ArrayList<>();
        keySchemaElements.add(new KeySchemaElement(FILE_NAME, KeyType.HASH));
        initialiseTable(activeFileInfoTablename, attributeDefinitions, keySchemaElements);
        initialiseTable(readyForGCFileInfoTablename, attributeDefinitions, keySchemaElements);
    }

    public void createPartitionInfoTable() {
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition(PARTITION_ID, ScalarAttributeType.S));
        List<KeySchemaElement> keySchemaElements = new ArrayList<>();
        keySchemaElements.add(new KeySchemaElement(PARTITION_ID, KeyType.HASH));
        initialiseTable(partitionTableName, attributeDefinitions, keySchemaElements);
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
