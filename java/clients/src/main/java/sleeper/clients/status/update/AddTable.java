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

package sleeper.clients.status.update;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;

import sleeper.configuration.s3properties.S3InstanceProperties;
import sleeper.configuration.s3properties.S3TableProperties;
import sleeper.configuration.statestore.StateStoreProvider;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.schema.Schema;
import sleeper.statestore.InitialiseStateStoreFromSplitPoints;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.nio.file.Path;

import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.PropertiesUtils.loadProperties;
import static sleeper.io.parquet.utils.HadoopConfigurationProvider.getConfigurationForClient;

public class AddTable {
    private final TableProperties tableProperties;
    private final TablePropertiesStore tablePropertiesStore;
    private final StateStoreProvider stateStoreProvider;

    public AddTable(
            AmazonS3 s3Client, AmazonDynamoDB dynamoDB,
            InstanceProperties instanceProperties, TableProperties tableProperties) {
        this(s3Client, dynamoDB, instanceProperties, tableProperties, getConfigurationForClient(instanceProperties));
    }

    public AddTable(
            AmazonS3 s3Client, AmazonDynamoDB dynamoDB,
            InstanceProperties instanceProperties, TableProperties tableProperties, Configuration configuration) {
        this.tableProperties = tableProperties;
        this.tablePropertiesStore = S3TableProperties.getStore(instanceProperties, s3Client, dynamoDB);
        this.stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoDB, configuration);
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

        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        try {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TableProperties tableProperties = new TableProperties(instanceProperties, loadProperties(tablePropertiesFile));
            tableProperties.setSchema(Schema.load(schemaFile));

            new AddTable(s3Client, dynamoDBClient, instanceProperties, tableProperties).run();
        } finally {
            s3Client.shutdown();
            dynamoDBClient.shutdown();
        }
    }
}
