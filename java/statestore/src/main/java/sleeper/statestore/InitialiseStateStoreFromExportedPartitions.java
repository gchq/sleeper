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
package sleeper.statestore;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionSerDe;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Initialises a {@link StateStore} from a file of partitions created using ExportPartitions
 * to export the partition information from an existing table.
 */
public class InitialiseStateStoreFromExportedPartitions {

    private InitialiseStateStoreFromExportedPartitions() {
    }

    public static void main(String[] args) throws StateStoreException, IOException {
        if (3 != args.length) {
            System.out.println("Usage: <instance-id> <table-name> <partitions-file>");
            return;
        }

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(s3Client, args[0]);
        TableProperties tableProperties = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient).getByName(args[1]);

        Configuration conf = new Configuration();
        conf.set("fs.s3a.aws.credentials.provider", DefaultAWSCredentialsProviderChain.class.getName());
        StateStore stateStore = new StateStoreFactory(dynamoDBClient, instanceProperties, conf).getStateStore(tableProperties);

        PartitionSerDe partitionSerDe = new PartitionSerDe(tableProperties.getSchema());
        List<Partition> partitions = new ArrayList<>();
        System.out.println("Attempting to read partitions from file " + args[2]);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(args[2]), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isEmpty()) {
                    partitions.add(partitionSerDe.fromJson(line));
                }
            }
        }
        System.out.println("Read " + partitions.size() + " partitions from file");

        stateStore.initialise(partitions);

        dynamoDBClient.shutdown();
        s3Client.shutdown();
    }
}
