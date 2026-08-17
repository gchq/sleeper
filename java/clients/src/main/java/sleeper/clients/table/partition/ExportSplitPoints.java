/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.clients.table.partition;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.StateStoreFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.local.WriteSplitPoints.writeSplitPoints;

/**
 * Allows the split points to be exported from a table. They can then be used
 * to initialise another table with the same partitions (note though that
 * this only exports split points in the first dimension).
 */
public class ExportSplitPoints {
    private final StateStore stateStore;
    private final Schema schema;

    public ExportSplitPoints(StateStore stateStore, Schema schema) {
        this.stateStore = stateStore;
        this.schema = schema;
    }

    public List<Object> getSplitPoints() {
        return new PartitionTree(stateStore.getAllPartitions()).getSplitPoints(schema);
    }

    public static void main(String[] args) throws IOException {
        if (3 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id> <table-name> <output-file>");
        }
        String instanceId = args[0];
        String tableName = args[1];
        Path outputFile = Paths.get(args[2]);

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                StsClient stsClient = buildAwsV2Client(StsClient.builder())) {
            String accountName = stsClient.getCallerIdentity().account();
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
            TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
            TableProperties tableProperties = tablePropertiesProvider.getByName(tableName);
            StateStoreFactory stateStoreFactory = new StateStoreFactory(instanceProperties, s3Client, dynamoClient);
            StateStore stateStore = stateStoreFactory.getStateStore(tablePropertiesProvider.getByName(tableName));
            ExportSplitPoints exportSplitPoints = new ExportSplitPoints(stateStore, tableProperties.getSchema());
            List<Object> splitPoints = exportSplitPoints.getSplitPoints();

            try (BufferedWriter writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {
                writeSplitPoints(splitPoints, writer, false);
            }
        }
    }
}
