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
package sleeper.statestore;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.transactionlog.transaction.impl.InitialisePartitionsTransaction;

import java.io.IOException;
import java.util.List;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.local.ReadSplitPoints.readSplitPoints;

/**
 * Initialises a state store. If a file of split points is provided then these are used to create the initial
 * {@link Partition}s. Each line of the file should contain a single point which is a split in the first dimension of
 * the row key. Only splitting by the first dimension is supported. If a file isn't provided then a single root
 * {@link Partition} is created.
 */
public class InitialiseStateStoreFromSplitPoints {
    private final StateStoreProvider stateStoreProvider;
    private final TableProperties tableProperties;
    private final List<Object> splitPoints;

    public InitialiseStateStoreFromSplitPoints(
            StateStoreProvider stateStoreProvider, TableProperties tableProperties) throws IOException {
        this(stateStoreProvider, tableProperties, readSplitPoints(tableProperties));
    }

    public InitialiseStateStoreFromSplitPoints(
            StateStoreProvider stateStoreProvider, TableProperties tableProperties, List<Object> splitPoints) {
        this.stateStoreProvider = stateStoreProvider;
        this.tableProperties = tableProperties;
        this.splitPoints = splitPoints;
    }

    /**
     * Initialises the state store.
     */
    public void run() {
        List<Partition> partitions = new PartitionsFromSplitPoints(tableProperties.getSchema(), splitPoints).construct();
        new InitialisePartitionsTransaction(partitions).synchronousCommit(stateStoreProvider.getStateStore(tableProperties));
    }

    /**
     * Initialises a state store from the command line.
     *
     * @param  args        the command line arguments
     * @throws IOException if the split points file could not be read
     */
    public static void main(String[] args) throws IOException {
        if (2 != args.length && 3 != args.length && 4 != args.length) {
            System.out.println("Usage: <instance-id> <table-name> <optional split points file> <optional boolean strings base64 encoded>");
            return;
        }
        String instanceId = args[0];
        String tableName = args[1];

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoDBClient = buildAwsV2Client(DynamoDbClient.builder())) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TableProperties tableProperties = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDBClient).getByName(tableName);

            List<Object> splitPoints = null;
            if (args.length > 2) {
                String splitPointsFile = args[2];
                boolean stringsBase64Encoded = 4 == args.length && Boolean.parseBoolean(args[2]);
                splitPoints = readSplitPoints(tableProperties, splitPointsFile, stringsBase64Encoded);
            }

            StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoDBClient);

            new InitialiseStateStoreFromSplitPoints(stateStoreProvider, tableProperties, splitPoints).run();
        }
    }
}
