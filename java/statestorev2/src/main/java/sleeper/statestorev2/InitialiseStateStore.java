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
package sleeper.statestorev2;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.transactionlog.transaction.impl.InitialisePartitionsTransaction;

import static sleeper.configurationv2.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * Initialises a state store with a single root partition.
 */
public class InitialiseStateStore {

    private InitialiseStateStore() {
    }

    /**
     * Initialises a state store with a single root partition from the command line.
     *
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        if (2 != args.length) {
            System.out.println("Usage: <instance-id> <table-name>");
            return;
        }
        String instanceId = args[0];
        String tableName = args[1];

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoDBClient = buildAwsV2Client(DynamoDbClient.builder());
                S3AsyncClient s3AsyncClient = buildAwsV2Client(S3AsyncClient.builder());
                S3TransferManager s3TransferManager = S3TransferManager.builder().s3Client(s3AsyncClient).build()) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);

            TableProperties tableProperties = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDBClient).getByName(tableName);

            StateStore stateStore = new StateStoreFactory(instanceProperties, s3Client, dynamoDBClient, s3TransferManager).getStateStore(tableProperties);

            InitialisePartitionsTransaction.singlePartition(tableProperties.getSchema()).synchronousCommit(stateStore);
        }
    }
}
