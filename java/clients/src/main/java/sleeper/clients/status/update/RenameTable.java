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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.configuration.s3properties.S3InstanceProperties;
import sleeper.configuration.s3properties.S3TableProperties;

import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

public class RenameTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RenameTable.class);

    private final TablePropertiesStore tablePropertiesStore;

    public RenameTable(AmazonS3 s3Client, AmazonDynamoDB dynamoDB, InstanceProperties instanceProperties) {
        this(S3TableProperties.getStore(instanceProperties, s3Client, dynamoDB));
    }

    public RenameTable(TablePropertiesStore tablePropertiesStore) {
        this.tablePropertiesStore = tablePropertiesStore;
    }

    public void rename(String oldName, String newName) {
        TableProperties tableProperties = tablePropertiesStore.loadByName(oldName);
        tableProperties.set(TABLE_NAME, newName);
        tablePropertiesStore.save(tableProperties);
        LOGGER.info("Successfully renamed table from {} to {}", oldName, newName);
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: <instance-id> <old-table-name> <new-table-name>");
        }
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        try {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, args[0]);
            new RenameTable(s3Client, dynamoDBClient, instanceProperties).rename(args[1], args[2]);
        } finally {
            dynamoDBClient.shutdown();
            s3Client.shutdown();
        }
    }
}
