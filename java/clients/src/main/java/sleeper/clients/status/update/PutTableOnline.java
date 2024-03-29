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
import sleeper.configuration.properties.table.S3TableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesStore;

import static sleeper.configuration.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

public class PutTableOnline {
    private static final Logger LOGGER = LoggerFactory.getLogger(PutTableOnline.class);

    private final TablePropertiesStore tablePropertiesStore;

    public PutTableOnline(AmazonS3 s3, AmazonDynamoDB dynamoDB, InstanceProperties instanceProperties) {
        this.tablePropertiesStore = S3TableProperties.getStore(instanceProperties, s3, dynamoDB);
    }

    public void putOnline(String tableName) {
        TableProperties tableProperties = tablePropertiesStore.loadByName(tableName);
        tableProperties.set(TABLE_ONLINE, "true");
        tablePropertiesStore.save(tableProperties);
        LOGGER.info("Successfully put table online: {}", tableProperties.getStatus());
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: <instance-id> <table-name>");
        }
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        try {
            InstanceProperties instanceProperties = new InstanceProperties();
            instanceProperties.loadFromS3GivenInstanceId(s3Client, args[0]);
            new PutTableOnline(s3Client, dynamoDBClient, instanceProperties).putOnline(args[1]);
        } finally {
            dynamoDBClient.shutdown();
            s3Client.shutdown();
        }
    }
}
