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

import sleeper.clients.util.console.ConsoleInput;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.statestore.StateStoreProvider;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.StateStoreFactory;

import static sleeper.clients.util.BucketUtils.deleteAllObjectsInBucketWithPrefix;
import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.io.parquet.utils.HadoopConfigurationProvider.getConfigurationForClient;

public class DeleteTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTable.class);
    private final AmazonS3 s3Client;
    private final InstanceProperties instanceProperties;
    private final TablePropertiesStore tablePropertiesStore;
    private final StateStoreProvider stateStoreProvider;

    public DeleteTable(AmazonS3 s3Client, AmazonDynamoDB dynamoDB, InstanceProperties instanceProperties) {
        this(instanceProperties, s3Client, S3TableProperties.createStore(instanceProperties, s3Client, dynamoDB),
                StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoDB, getConfigurationForClient()));
    }

    public DeleteTable(InstanceProperties instanceProperties, AmazonS3 s3Client, TablePropertiesStore tablePropertiesStore, StateStoreProvider stateStoreProvider) {
        this.s3Client = s3Client;
        this.instanceProperties = instanceProperties;
        this.tablePropertiesStore = tablePropertiesStore;
        this.stateStoreProvider = stateStoreProvider;
    }

    public void delete(String tableName) throws StateStoreException {
        TableProperties tableProperties = tablePropertiesStore.loadByName(tableName);
        /*
         * - First we clear the state store for the table. This needs to happen first so that if a transaction log
         * snapshot exists, it can be loaded when the transaction log state store updates.
         * - We then delete all files in the data bucket for the table.
         * - Finally, the table is removed from the table index, and the table properties file is removed. This happens
         * last to handle the case where the deletion of files in the data bucket fails, so you can still see that a
         * table existed.
         */
        stateStoreProvider.getStateStore(tableProperties).clearSleeperTable();
        deleteAllObjectsInBucketWithPrefix(s3Client, instanceProperties.get(DATA_BUCKET), tableProperties.get(TABLE_ID));
        tablePropertiesStore.deleteByName(tableName);
        LOGGER.info("Successfully deleted table {}", tableProperties.getStatus());
    }

    public static void main(String[] args) throws StateStoreException {
        if (args.length < 2 || args.length > 3) {
            System.out.println("Usage: <instance-id> <table-name> <optional-force-flag>");
        }
        String tableName = args[1];
        boolean force = optionalArgument(args, 2).map("--force"::equals).orElse(false);
        if (!force) {
            ConsoleInput input = new ConsoleInput(System.console());
            String result = input.promptLine("Are you sure you want to delete the table " + tableName + "? [y/N]");
            if (!result.equalsIgnoreCase("y")) {
                return;
            }
        }
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        try {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, args[0]);
            new DeleteTable(s3Client, dynamoDBClient, instanceProperties).delete(tableName);
        } finally {
            dynamoDBClient.shutdown();
            s3Client.shutdown();
        }
    }
}
