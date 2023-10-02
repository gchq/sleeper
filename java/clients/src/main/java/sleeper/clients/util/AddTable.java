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

package sleeper.clients.util;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.statestore.InitialiseStateStoreFromSplitPoints;

import java.io.IOException;
import java.nio.file.Path;

import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class AddTable {
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoDB;
    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final TableProperties tableProperties;

    public AddTable(AmazonS3 s3Client, AmazonDynamoDB dynamoDB, InstanceProperties instanceProperties,
                    TableProperties tableProperties) {
        this.s3Client = s3Client;
        this.dynamoDB = dynamoDB;
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        this.tableProperties = tableProperties;
    }

    public void run() throws IOException {
        String tableName = tableProperties.get(TABLE_NAME);
        if (tablePropertiesProvider.getTablePropertiesIfExists(tableName).isPresent()) {
            throw new UnsupportedOperationException("Table with name " + tableName + " already exists");
        }
        tableProperties.saveToS3(s3Client);
        new InitialiseStateStoreFromSplitPoints(dynamoDB, instanceProperties, tableProperties).run();
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("Usage: <instance-id> <table-properties-file>");
            return;
        }

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, args[0]);

        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.load(Path.of(args[1]));

        new AddTable(s3Client, dynamoDBClient, instanceProperties, tableProperties).run();
        dynamoDBClient.shutdown();
        s3Client.shutdown();
    }
}
