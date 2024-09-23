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
package sleeper.clients.status.partitions;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.S3InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.partition.PartitionSerDe;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.StateStoreFactory;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

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

    public List<String> getPartitionsAsJsonStrings() throws StateStoreException {
        return stateStore.getAllPartitions()
                .stream()
                .map(p -> partitionSerDe.toJson(p, false))
                .collect(Collectors.toUnmodifiableList());
    }

    public void writePartitionsToFile(String filename) throws IOException, StateStoreException {
        List<String> partitionsAsJsonStrings = getPartitionsAsJsonStrings();
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), StandardCharsets.UTF_8))) {
            for (String partition : partitionsAsJsonStrings) {
                writer.write(partition);
                writer.write("\n");
            }
        }
        LOGGER.info("Wrote {} partitions to file {}", partitionsAsJsonStrings.size(), filename);
    }

    public static void main(String[] args) throws IOException, StateStoreException {
        if (3 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id> <table-name> <output-file>");
        }
        String instanceId = args[0];
        String tableName = args[1];
        String outputFile = args[2];

        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        try {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient);
            TableProperties tableProperties = tablePropertiesProvider.getByName(tableName);
            StateStoreFactory stateStoreFactory = new StateStoreFactory(instanceProperties, s3Client, dynamoDBClient, new Configuration());
            StateStore stateStore = stateStoreFactory.getStateStore(tablePropertiesProvider.getByName(tableName));
            ExportPartitions exportPartitions = new ExportPartitions(stateStore, tableProperties.getSchema());
            exportPartitions.writePartitionsToFile(outputFile);
        } finally {
            s3Client.shutdown();
            dynamoDBClient.shutdown();
        }
    }
}
