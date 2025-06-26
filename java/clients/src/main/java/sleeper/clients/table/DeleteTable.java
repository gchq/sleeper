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
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.util.console.ConsoleInput;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotMetadataStore;

import static sleeper.clients.util.BucketUtils.deleteAllObjectsInBucketWithPrefix;
import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

public class DeleteTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTable.class);

    private final InstanceProperties instanceProperties;
    private final S3Client s3Client;
    private final DynamoDbClient dynamoClient;
    private final TablePropertiesStore tablePropertiesStore;
    private final StateStoreProvider stateStoreProvider;

    public DeleteTable(InstanceProperties instanceProperties, S3Client s3Client, DynamoDbClient dynamoClient) {
        this.instanceProperties = instanceProperties;
        this.s3Client = s3Client;
        this.dynamoClient = dynamoClient;
        this.tablePropertiesStore = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);
        this.stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient);
    }

    public void delete(String tableName) {
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
        snapshotMetadataStore(tableProperties).deleteAllSnapshots();
        tablePropertiesStore.deleteByName(tableName);
        LOGGER.info("Successfully deleted table {}", tableProperties.getStatus());
    }

    private DynamoDBTransactionLogSnapshotMetadataStore snapshotMetadataStore(TableProperties tableProperties) {
        return new DynamoDBTransactionLogSnapshotMetadataStore(instanceProperties, tableProperties, dynamoClient);
    }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 3) {
            System.out.println("Usage: <instance-id> <table-name> <optional-force-flag>");
        }
        String tableName = args[1];
        boolean force = optionalArgument(args, 2).map("--force"::equals).orElse(false);
        if (!force) {
            ConsoleInput input = new ConsoleInput(System.console());
            String result = input.promptLine("Are you sure you want to delete the table " + tableName + "? [y/N]");
            if (!"y".equalsIgnoreCase(result)) {
                return;
            }
        }
        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder())) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, args[0]);
            new DeleteTable(instanceProperties, s3Client, dynamoClient).delete(tableName);
        }
    }
}
