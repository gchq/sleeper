/*
 * Copyright 2022 Crown Copyright
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
package sleeper.status.report;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.partition.Partition;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import sleeper.util.ClientUtils;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A utility class to report information about the partitions in the system and
 * their status.
 */
public class PartitionsStatusReport {
    private final StateStore stateStore;

    public PartitionsStatusReport(StateStore stateStore) {
        this.stateStore = stateStore;
    }

    public void run() throws StateStoreException {
        System.out.println("\nPartitions Status Report:\n--------------------------");
        List<Partition> partitions = stateStore.getAllPartitions();
        List<Partition> leafPartitions = partitions.stream().filter(Partition::isLeafPartition).collect(Collectors.toList());
        System.out.println("There are " + partitions.size() + " partitions (" + leafPartitions.size() + " leaf partitions)");
        partitions.stream().forEach(System.out::println);
    }

    public static void main(String[] args) throws IOException, StateStoreException {
        if (2 != args.length) {
            throw new IllegalArgumentException("Usage: <instance id> <table name>");
        }

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, args[0]);

        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(amazonS3, instanceProperties);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, new Configuration());
        StateStore stateStore = stateStoreProvider.getStateStore(args[1], tablePropertiesProvider);

        PartitionsStatusReport statusReport = new PartitionsStatusReport(stateStore);
        statusReport.run();

        amazonS3.shutdown();
        dynamoDBClient.shutdown();
    }
}
