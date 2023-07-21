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
package sleeper.cdk.custom;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.table.job.TableInitialiser;

import java.io.IOException;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class SleeperTableLambda {
    private AmazonS3 s3Client;
    private AmazonDynamoDB dynamoDBClient;
    private static final Logger LOGGER = LoggerFactory.getLogger(SleeperTableLambda.class);

    public SleeperTableLambda() {
        this(AmazonS3ClientBuilder.defaultClient(), AmazonDynamoDBClientBuilder.defaultClient());
    }

    public SleeperTableLambda(AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient) {
        this.s3Client = s3Client;
        this.dynamoDBClient = dynamoDBClient;
    }

    public void handleEvent(CloudFormationCustomResourceEvent event, Context context) throws IOException {
        LOGGER.info("Received event: {}", event);
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromString((String) event.getResourceProperties().get("instanceProperties"));
        TableProperties tableProperties = new TableProperties(instanceProperties);
        String configBucket = instanceProperties.get(CONFIG_BUCKET);
        tableProperties.loadFromString((String) event.getResourceProperties().get("tableProperties"));
        switch (event.getRequestType()) {
            case "Create":
                initialiseTable(instanceProperties, tableProperties, configBucket);
                break;
            case "Update":
                updateTableProperties(tableProperties);
                break;
            case "Delete":
                deleteTableProperties(tableProperties, configBucket);
                break;
            default:
                throw new IllegalArgumentException("Invalid request type: " + event.getRequestType());
        }
    }

    private void updateTableProperties(TableProperties tableProperties) throws IOException {
        LOGGER.info("Updating properties");
        tableProperties.saveToS3(s3Client);
    }

    private void deleteTableProperties(TableProperties tableProperties, String bucket) {
        LOGGER.info("Tearing down properties");
        s3Client.deleteObject(bucket, TableProperties.TABLES_PREFIX + "/" + tableProperties.get(TABLE_NAME));
    }

    private void initialiseTable(InstanceProperties instanceProperties, TableProperties tableProperties, String bucket) throws IOException {
        LOGGER.info("Initialising Table");
        // Initialise Table
        new TableInitialiser(s3Client, dynamoDBClient).initialise(instanceProperties, tableProperties, bucket, new Configuration());
        updateTableProperties(tableProperties);
    }
}
