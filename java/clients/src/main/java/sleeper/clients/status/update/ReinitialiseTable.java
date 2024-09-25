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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.s3properties.S3InstanceProperties;
import sleeper.configuration.s3properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.s3.S3StateStore;

import java.io.IOException;
import java.util.Objects;

import static sleeper.clients.util.BucketUtils.deleteObjectsInBucketWithPrefix;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

/**
 * A utility class to reinitialise a table by first deleting the table's contents
 * and the state store items related to them. Then the state store for the table
 * is reinitialised.
 */
public class ReinitialiseTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReinitialiseTable.class);
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoDBClient;
    private final boolean deletePartitions;
    private final String instanceId;
    private final String tableName;

    public ReinitialiseTable(
            AmazonS3 s3Client,
            AmazonDynamoDB dynamoDBClient,
            String instanceId,
            String tableName,
            boolean deletePartitions) {
        this.s3Client = s3Client;
        this.dynamoDBClient = dynamoDBClient;
        this.deletePartitions = deletePartitions;
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId must not be null");
        this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
        if (instanceId.isEmpty() || tableName.isEmpty()) {
            throw new IllegalArgumentException("You have tried to create a ReinitialiseTable class with " +
                    "an empty String in the instance id or table name. These must not be empty.");
        }
    }

    public void run() throws IOException, StateStoreException {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
        TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDBClient);
        TableProperties tableProperties = tablePropertiesProvider.getByName(tableName);

        Configuration conf = new Configuration();
        conf.set("fs.s3a.aws.credentials.provider", DefaultAWSCredentialsProviderChain.class.getName());

        StateStore stateStore = new StateStoreFactory(instanceProperties, s3Client, dynamoDBClient, conf)
                .getStateStore(tableProperties);

        LOGGER.info("State store type: {}", stateStore.getClass().getName());

        if (deletePartitions) {
            stateStore.clearSleeperTable();
        } else {
            stateStore.clearFileData();
        }
        deleteObjectsInBucketWithPrefix(s3Client, instanceProperties.get(DATA_BUCKET), tableProperties.get(TABLE_ID),
                key -> key.matches(tableProperties.get(TABLE_ID) + "/partition.*/.*"));
        if (deletePartitions) {
            LOGGER.info("Fully reinitialising table");
            initialiseStateStore(tableProperties, stateStore);
        } else if (stateStore instanceof S3StateStore) {
            LOGGER.info("Recreating files information file and adding it into the revisions table");
            S3StateStore s3StateStore = (S3StateStore) stateStore;
            s3StateStore.setInitialFileReferences();
        }
    }

    protected void initialiseStateStore(TableProperties tableProperties, StateStore stateStore) throws IOException, StateStoreException {
        stateStore.initialise();
    }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 3) {
            throw new IllegalArgumentException("Usage: <instance-id> <table-name> <optional-delete-partitions-true-or-false>");
        }
        String instanceId = args[0];
        String tableName = args[1];
        boolean deletePartitions = args.length != 2 && Boolean.parseBoolean(args[2]);

        System.out.println("If you continue all data will be deleted in the table.");
        if (deletePartitions) {
            System.out.println("The metadata about the partitions will be deleted and the "
                    + "table will be reset to consist of one root partition.");
        } else {
            System.out.println("The metadata about the partitions will not be deleted.");
        }
        String choice = System.console().readLine("Are you sure you want to delete the data and " +
                "reinitialise this table?\nPlease enter Y or N: ");
        if (!choice.equalsIgnoreCase("y")) {
            System.exit(0);
        }
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());

        try {
            ReinitialiseTable reinitialiseTable = new ReinitialiseTable(s3Client, dynamoDBClient, instanceId, tableName, deletePartitions);
            reinitialiseTable.run();
            LOGGER.info("Table reinitialised successfully");
        } catch (RuntimeException | IOException | StateStoreException e) {
            LOGGER.error("\nAn Error occurred while trying to reinitialise the table. " +
                    "The error message is as follows:\n\n" + e.getMessage()
                    + "\n\nCause:" + e.getCause());
        } finally {
            s3Client.shutdown();
            dynamoDBClient.shutdown();
        }
    }
}
