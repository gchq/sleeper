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
import sleeper.statestore.InitialiseStateStoreFromSplitPoints;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotMetadataStore;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

/**
 * Lambda Function which defines Sleeper tables.
 */
public class TableDefinerLambda {
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesWriterLambda.class);
    private final S3Client s3Client;
    private final DynamoDbClient dynamoClient;
    private final String bucketName;
    private InstanceProperties instanceProperties;
    private TableProperties tableProperties;
    private TablePropertiesStore tablePropertiesStore;
    private StateStoreProvider stateStoreProvider;

    public TableDefinerLambda() {
        this(S3Client.create(), DynamoDbClient.create(), System.getenv(CONFIG_BUCKET.toEnvironmentVariable()));
    }

    public TableDefinerLambda(S3Client s3Client, DynamoDbClient dynamoClient, String bucketName) {
        this.s3Client = s3Client;
        this.dynamoClient = dynamoClient;
        this.bucketName = bucketName;
    }

    public void handleEvent(CloudFormationCustomResourceEvent event, Context context) throws IOException {
        Map<String, Object> resourceProperties = event.getResourceProperties();
        loadProperties(resourceProperties);
        switch (event.getRequestType()) {
            case "Create":
                addTable(resourceProperties);
                break;
            case "Update":
                updateTable(resourceProperties);
                break;
            case "Delete":
                deleteTable();
                break;
            default:
                throw new IllegalArgumentException("Invalid request type: " + event.getRequestType());
        }
    }

    private void loadProperties(Map<String, Object> resourceProperties) throws IOException {
        instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, bucketName);
        stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient);
        tablePropertiesStore = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);

        Properties properties = new Properties();
        properties.load(new StringReader((String) resourceProperties.get("tableProperties")));
        tableProperties = new TableProperties(instanceProperties, properties);

        LOGGER.info("Validating table properties for table {}", tableProperties.get(TABLE_NAME));
        tableProperties.validate();
    }

    private void addTable(Map<String, Object> resourceProperties) throws IOException {
        String tableName = tableProperties.get(TableProperty.TABLE_NAME);

        LOGGER.info("Creating table {}", tableName);
        tablePropertiesStore.createTable(tableProperties);

        List<Object> splitPoints = ReadSplitPoints.fromString((String) resourceProperties.get("splitPoints"),
                tableProperties.getSchema(),
                tableProperties.getBoolean(TableProperty.SPLIT_POINTS_BASE64_ENCODED));
        LOGGER.info("Initalising state store from split points for table {}", tableName);
        new InitialiseStateStoreFromSplitPoints(stateStoreProvider, tableProperties, splitPoints).run();
    }

    private void updateTable(Map<String, Object> resourceProperties) {
        //TODO
    }

    private void deleteTable() {
        stateStoreProvider.getStateStore(tableProperties).clearSleeperTable();
        //deleteAllObjectsInBucketWithPrefix(s3Client, instanceProperties.get(DATA_BUCKET), tableProperties.get(TABLE_ID));
        new DynamoDBTransactionLogSnapshotMetadataStore(instanceProperties, tableProperties, dynamoClient).deleteAllSnapshots();
        tablePropertiesStore.deleteByName(tableProperties.get(TABLE_NAME));
    }
}
