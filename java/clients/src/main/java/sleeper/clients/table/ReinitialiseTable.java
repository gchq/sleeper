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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.transactionlog.transaction.impl.ClearFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.ClearPartitionsTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.InitialisePartitionsTransaction;
import sleeper.statestorev2.StateStoreFactory;

import java.util.Objects;
import java.util.function.Function;

import static sleeper.clients.util.BucketUtils.deleteObjectsInBucketWithPrefix;
import static sleeper.configurationv2.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

/**
 * A utility class to reinitialise a table by first deleting the table's contents
 * and the state store items related to them. Then the state store for the table
 * is reinitialised.
 */
public class ReinitialiseTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReinitialiseTable.class);
    private final S3Client s3Client;
    private final S3TransferManager s3TransferManager;
    private final DynamoDbClient dynamoClient;
    private final boolean deletePartitions;
    private final String instanceId;
    private final String tableName;

    public ReinitialiseTable(
            S3Client s3Client,
            S3TransferManager s3TransferManager,
            DynamoDbClient dynamoClient,
            String instanceId,
            String tableName,
            boolean deletePartitions) {
        this.s3Client = s3Client;
        this.s3TransferManager = s3TransferManager;
        this.dynamoClient = dynamoClient;
        this.deletePartitions = deletePartitions;
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId must not be null");
        this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
        if (instanceId.isEmpty() || tableName.isEmpty()) {
            throw new IllegalArgumentException("You have tried to create a ReinitialiseTable class with " +
                    "an empty String in the instance id or table name. These must not be empty.");
        }
    }

    public void run() {
        run(tableProperties -> InitialisePartitionsTransaction.singlePartition(tableProperties.getSchema()));
    }

    public void run(Function<TableProperties, InitialisePartitionsTransaction> buildPartitions) {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
        TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
        TableProperties tableProperties = tablePropertiesProvider.getByName(tableName);

        StateStore stateStore = new StateStoreFactory(instanceProperties, s3Client, dynamoClient, s3TransferManager)
                .getStateStore(tableProperties);

        LOGGER.info("State store type: {}", stateStore.getClass().getName());

        new ClearFilesTransaction().synchronousCommit(stateStore);
        if (deletePartitions) {
            ClearPartitionsTransaction.create().synchronousCommit(stateStore);
        }
        deleteObjectsInBucketWithPrefix(s3Client, instanceProperties.get(DATA_BUCKET), tableProperties.get(TABLE_ID),
                key -> key.matches(tableProperties.get(TABLE_ID) + "/partition.*/.*"));
        if (deletePartitions) {
            LOGGER.info("Fully reinitialising table");
            buildPartitions.apply(tableProperties).synchronousCommit(stateStore);
        }
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

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                S3AsyncClient s3AsyncClient = buildAwsV2Client(S3AsyncClient.crtBuilder());
                S3TransferManager s3TransferManager = S3TransferManager.builder().s3Client(s3AsyncClient).build();
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder())) {
            ReinitialiseTable reinitialiseTable = new ReinitialiseTable(s3Client, s3TransferManager, dynamoClient, instanceId, tableName, deletePartitions);
            reinitialiseTable.run();
            LOGGER.info("Table reinitialised successfully");
        } catch (RuntimeException e) {
            LOGGER.error("\nAn Error occurred while trying to reinitialise the table. " +
                    "The error message is as follows:\n\n" + e.getMessage()
                    + "\n\nCause:" + e.getCause());
        }
    }
}
