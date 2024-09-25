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
package sleeper.statestore;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;

import sleeper.configuration.s3properties.S3InstanceProperties;
import sleeper.configuration.s3properties.S3TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionSerDe;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

/**
 * Initialises a state store from a file of exported partitions. These can be created using ExportPartitions to export
 * from an existing Sleeper table.
 */
public class InitialiseStateStoreFromExportedPartitions {

    private InitialiseStateStoreFromExportedPartitions() {
    }

    /**
     * Initialises a state store from exported partitions from the command line.
     *
     * @param  args                the command line arguments
     * @throws StateStoreException if the state store initialisation fails
     * @throws IOException         if we could not read the partitions file
     */
    public static void main(String[] args) throws StateStoreException, IOException {
        if (3 != args.length) {
            System.out.println("Usage: <instance-id> <table-name> <partitions-file>");
            return;
        }
        String instanceId = args[0];
        String tableName = args[1];
        Path partitionsFile = Path.of(args[2]);

        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        try {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TableProperties tableProperties = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDBClient).getByName(tableName);

            Configuration conf = HadoopConfigurationProvider.getConfigurationForClient();
            StateStore stateStore = new StateStoreFactory(instanceProperties, s3Client, dynamoDBClient, conf).getStateStore(tableProperties);

            PartitionSerDe partitionSerDe = new PartitionSerDe(tableProperties.getSchema());
            List<Partition> partitions = new ArrayList<>();
            System.out.println("Attempting to read partitions from file " + partitionsFile);
            try (BufferedReader reader = Files.newBufferedReader(partitionsFile, StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.isEmpty()) {
                        partitions.add(partitionSerDe.fromJson(line));
                    }
                }
            }
            System.out.println("Read " + partitions.size() + " partitions from file");

            stateStore.initialise(partitions);
        } finally {
            dynamoDBClient.shutdown();
            s3Client.shutdown();
        }
    }
}
