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
package sleeper.clients.table.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.partition.PartitionSerDe;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.StateStoreFactory;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * Allows the metadata about the partitions in a table to be output to a text
 * file with each partition written as JSON on a single line. This file can then
 * be used to initialise another table with the same partitions.
 */
public class ExportPartitions {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExportPartitions.class);

    private final StateStore stateStore;
    private final PartitionSerDe partitionSerDe;

    public ExportPartitions(StateStore stateStore, Schema schema) {
        this.stateStore = stateStore;
        this.partitionSerDe = new PartitionSerDe(schema);
    }

    public List<String> getPartitionsAsJsonStrings() {
        return stateStore.getAllPartitions()
                .stream()
                .map(p -> partitionSerDe.toJson(p, false))
                .collect(Collectors.toUnmodifiableList());
    }

    public void writePartitionsToFile(String filename) throws IOException {
        List<String> partitionsAsJsonStrings = getPartitionsAsJsonStrings();
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), StandardCharsets.UTF_8))) {
            for (String partition : partitionsAsJsonStrings) {
                writer.write(partition);
                writer.write("\n");
            }
        }
        LOGGER.info("Wrote {} partitions to file {}", partitionsAsJsonStrings.size(), filename);
    }

    public static void main(String[] args) throws IOException {
        if (3 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id> <table-name> <output-file>");
        }
        String instanceId = args[0];
        String tableName = args[1];
        String outputFile = args[2];

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder())) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
            TableProperties tableProperties = tablePropertiesProvider.getByName(tableName);
            StateStoreFactory stateStoreFactory = new StateStoreFactory(instanceProperties, s3Client, dynamoClient);
            StateStore stateStore = stateStoreFactory.getStateStore(tablePropertiesProvider.getByName(tableName));
            ExportPartitions exportPartitions = new ExportPartitions(stateStore, tableProperties.getSchema());
            exportPartitions.writePartitionsToFile(outputFile);
        }
    }
}
