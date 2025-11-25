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
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.table.TableIdGenerator;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;
import java.util.Properties;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

/**
 * Lambda Function which defines sleeper tables.
 */
public class TableDefinerLambda {
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesWriterLambda.class);
    private final S3Client s3Client;
    private final DynamoDbClient dynamoClient;

    public TableDefinerLambda() {
        this(S3Client.create(), DynamoDbClient.create());
    }

    public TableDefinerLambda(S3Client s3Client, DynamoDbClient dynamoClient) {
        this.s3Client = s3Client;
        this.dynamoClient = dynamoClient;
    }

    public void handleEvent(CloudFormationCustomResourceEvent event, Context context) throws IOException {
        Map<String, Object> resourceProperties = event.getResourceProperties();
        String instanceId = (String) resourceProperties.get("instanceId");
        String tableName = (String) resourceProperties.get("tableName");
        switch (event.getRequestType()) {
            case "Create":
                addTable(instanceId, tableName, resourceProperties);
                break;
            case "Update":
                updateTable(instanceId, tableName, resourceProperties);
                break;
            case "Delete":
                deleteTable(instanceId, tableName);
                break;
            default:
                throw new IllegalArgumentException("Invalid request type: " + event.getRequestType());
        }
    }

    /*
     * instanceId: id
     * tableName: Foo
     * tableProperties: {
     * some: bar
     * other: ram
     * }
     * splitPoints: {}
     * schema: {
     * rowkeyFields: row
     * sortKeyFields: sort
     * valueFields: value
     * }
     */
    private void addTable(String instanceId, String tableName, Map<String, Object> resourceProperties) {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
        Properties properties = new Properties();
        properties.load(new StringReader((String) resourceProperties.get("tableProperties")));
        TableProperties tableProperties = new TableProperties(instanceProperties, properties);
        String tableId = new TableIdGenerator().generateString();
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.set(TABLE_ID, tableId);
        tableProperties.setSchema(Schema.loadFromString((String) resourceProperties.get("schema")));

        TablePropertiesStore tablePropertiesStore = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);
        StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient);
        tableProperties.validate();
        tablePropertiesStore.createTable(tableProperties);
        new InitialiseStateStoreFromSplitPoints(stateStoreProvider, tableProperties).run();
    }

    private void updateTable(String instanceId, String tableName, Map<String, Object> resourceProperties) {
        //TODO
    }

    private void deleteTable(String instanceId, String tableName) {
        //TODO
    }
}
