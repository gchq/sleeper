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
package sleeper.clients.status.partitions;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.util.ClientUtils;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.partition.PartitionSerDe;
import sleeper.core.schema.Schema;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

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

    public void writePartitionsToFile(String filename) throws FileNotFoundException, IOException, StateStoreException {
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
            throw new IllegalArgumentException("Usage: <instance id> <table name> <output file>");
        }

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, args[0]);

        String tableName = args[1];
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(amazonS3, instanceProperties);
        TableProperties tableProperties = tablePropertiesProvider.getTableProperties(tableName);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, new Configuration());
        StateStore stateStore = stateStoreProvider.getStateStore(tableName, tablePropertiesProvider);
        ExportPartitions exportPartitions = new ExportPartitions(stateStore, tableProperties.getSchema());
        exportPartitions.writePartitionsToFile(args[2]);

        amazonS3.shutdown();
        dynamoDBClient.shutdown();
    }
}
