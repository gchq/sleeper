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

package sleeper.clients.table;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.statestorev2.InitialiseStateStoreFromSplitPoints;
import sleeper.statestorev2.StateStoreFactory;

import java.io.IOException;
import java.nio.file.Path;

import static sleeper.configurationv2.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.PropertiesUtils.loadProperties;

public class AddTable {
    private final TableProperties tableProperties;
    private final TablePropertiesStore tablePropertiesStore;
    private final StateStoreProvider stateStoreProvider;

    public AddTable(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            TablePropertiesStore tablePropertiesStore, StateStoreProvider stateStoreProvider) {
        this.tableProperties = tableProperties;
        this.tablePropertiesStore = tablePropertiesStore;
        this.stateStoreProvider = stateStoreProvider;
    }

    public void run() throws IOException {
        tableProperties.validate();
        tablePropertiesStore.createTable(tableProperties);
        new InitialiseStateStoreFromSplitPoints(stateStoreProvider, tableProperties).run();
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println("Usage: <instance-id> <table-properties-file> <schema-file>");
            return;
        }
        String instanceId = args[0];
        Path tablePropertiesFile = Path.of(args[1]);
        Path schemaFile = Path.of(args[2]);

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                S3AsyncClient s3AsyncClient = buildAwsV2Client(S3AsyncClient.crtBuilder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder())) {

            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TableProperties tableProperties = new TableProperties(instanceProperties, loadProperties(tablePropertiesFile));
            tableProperties.setSchema(Schema.load(schemaFile));

            TablePropertiesStore tablePropertiesStore = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);
            StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient);
            new AddTable(instanceProperties, tableProperties, tablePropertiesStore, stateStoreProvider).run();
        }
    }
}
