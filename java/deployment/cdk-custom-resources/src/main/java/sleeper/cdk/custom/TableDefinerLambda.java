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
package sleeper.cdk.custom;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.ReadSplitPoints;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.table.TableAlreadyExistsException;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.table.TableStatus;
import sleeper.statestore.InitialiseStateStoreFromSplitPoints;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotMetadataStore;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static sleeper.configuration.utils.BucketUtils.deleteAllObjectsInBucketWithPrefix;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.RETAIN_TABLE_AFTER_REMOVAL;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;

/**
 * Lambda Function which defines Sleeper tables.
 */
public class TableDefinerLambda {
    private static final Logger LOGGER = LoggerFactory.getLogger(InstancePropertiesWriterLambda.class);
    private final S3Client s3Client;
    private final DynamoDbClient dynamoClient;
    private final String bucketName;

    public TableDefinerLambda() {
        this(S3Client.create(), DynamoDbClient.create(), System.getenv(CONFIG_BUCKET.toEnvironmentVariable()));
    }

    public TableDefinerLambda(S3Client s3Client, DynamoDbClient dynamoClient, String bucketName) {
        this.s3Client = s3Client;
        this.dynamoClient = dynamoClient;
        this.bucketName = bucketName;
    }

    public void handleEvent(CloudFormationCustomResourceEvent event, Context context) throws IOException {
        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, bucketName);
        TablePropertiesStore tablePropertiesStore = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);
        Map<String, Object> resourceProperties = event.getResourceProperties();

        Properties properties = new Properties();
        properties.load(new StringReader((String) resourceProperties.get("tableProperties")));
        TableProperties tableProperties = new TableProperties(instanceProperties, properties);

        switch (event.getRequestType()) {
            case "Create":
                addTable(tableProperties, tablePropertiesStore, resourceProperties);
                break;
            case "Update":
                LOGGER.info("Updating table properties for table {}", tableProperties.get(TABLE_NAME));
                updateTable(tableProperties, tablePropertiesStore);
                break;
            case "Delete":
                deleteTable(tableProperties, tablePropertiesStore);
                break;
            default:
                throw new IllegalArgumentException("Invalid request type: " + event.getRequestType());
        }
    }

    private void addTable(TableProperties tableProperties, TablePropertiesStore tablePropertiesStore,
            Map<String, Object> resourceProperties) throws IOException {
        String tableName = tableProperties.get(TABLE_NAME);

        LOGGER.info("Validating table properties for table {}", tableName);
        tableProperties.validate();

        //Table may just be offline from a previous delete call
        if (tableProperties.getBoolean(TableProperty.TABLE_REUSE_EXISTING)) {
            reuseExistingTable(tableName, tablePropertiesStore, tableProperties);
        } else {
            createNewTable(tableName, tablePropertiesStore, tableProperties, resourceProperties);
        }
    }

    private void reuseExistingTable(String tableName, TablePropertiesStore tablePropertiesStore, TableProperties tableProperties) {
        LOGGER.info("Table {} expected to already exist. Attempting to update its properties", tableName);
        try {
            tablePropertiesStore.update(tableProperties);
        } catch (TableNotFoundException e) {
            throw new NoTableToReuseException(tableName, e);
        }
    }

    private void createNewTable(String tableName, TablePropertiesStore tablePropertiesStore, TableProperties tableProperties,
            Map<String, Object> resourceProperties) {
        LOGGER.info("Creating new table {}", tableName);
        try {
            tablePropertiesStore.createTable(tableProperties);
        } catch (TableAlreadyExistsException e) {
            throw new TableAlreadyExistsException(e.getMessage() + ". If attempting to reuse an existing table " +
                    "ensure the sleeper.table.reuse.existing property is set to true.", e);
        }

        List<Object> splitPoints = ReadSplitPoints.fromString((String) resourceProperties.get("splitPoints"),
                tableProperties.getSchema(),
                tableProperties.getBoolean(TableProperty.SPLIT_POINTS_BASE64_ENCODED));
        LOGGER.info("Initalising state store from split points for table {}", tableName);
        StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(
                tableProperties.getInstanceProperties(), s3Client, dynamoClient);
        new InitialiseStateStoreFromSplitPoints(stateStoreProvider, tableProperties, splitPoints).run();
    }

    private void deleteTable(TableProperties tableProperties, TablePropertiesStore tablePropertiesStore) {
        String tableName = tableProperties.get(TABLE_NAME);
        if (tableProperties.getBoolean(RETAIN_TABLE_AFTER_REMOVAL)) {
            LOGGER.info("Taking table {} offline.", tableName);
            tableProperties.set(TABLE_ONLINE, "false");
            tablePropertiesStore.save(tableProperties);
        } else {
            //Need to look up full properties to get the ID for deleting objects in bucket with prefix.
            tableProperties = tablePropertiesStore.loadByName(tableName);
            String tableId = tableProperties.get(TABLE_ID);
            InstanceProperties instanceProperties = tableProperties.getInstanceProperties();
            LOGGER.info("Deleting table {} and associated data.", tableName);
            StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient)
                    .getStateStore(tableProperties).clearSleeperTable();
            deleteAllObjectsInBucketWithPrefix(s3Client, instanceProperties.get(DATA_BUCKET), tableId);
            new DynamoDBTransactionLogSnapshotMetadataStore(instanceProperties, tableProperties, dynamoClient).deleteAllSnapshots();
            tablePropertiesStore.delete(TableStatus.uniqueIdAndName(tableId, tableName, tableProperties.getBoolean(TABLE_ONLINE)));
        }
    }

    private void updateTable(TableProperties tableProperties, TablePropertiesStore tablePropertiesStore) {
        Optional<TableStatus> existingOpt = tablePropertiesStore.getExistingStatus(tableProperties);
        String tableId = tableProperties.get(TABLE_ID);
        String tableName = tableProperties.get(TABLE_NAME);
        TableProperties existingTableProperties;

        if (tableId.isEmpty()) {
            existingTableProperties = tablePropertiesStore.loadByName(tableName);
        } else {
            existingTableProperties = tablePropertiesStore.loadById(tableId);
        }

        if (!existingTableProperties.get(TABLE_NAME).equals(tableName)) {
            tableProperties.set(TABLE_NAME, tableName);
        }

        tableProperties.validate();
        tablePropertiesStore.updateTable(existingOpt.get(), tableProperties);
    }

}
