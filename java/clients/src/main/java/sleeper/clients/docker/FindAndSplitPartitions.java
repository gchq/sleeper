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

package sleeper.clients.docker;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.splitter.FindPartitionsToSplit;
import sleeper.splitter.SplitPartition;
import sleeper.splitter.SplitPartitionJobDefinition;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;
import static sleeper.io.parquet.utils.HadoopConfigurationProvider.getConfigurationForClient;

public class FindAndSplitPartitions {
    private static final Logger LOGGER = LoggerFactory.getLogger(FindAndSplitPartitions.class);

    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final Configuration configuration;

    public FindAndSplitPartitions(InstanceProperties instanceProperties,
                                  TablePropertiesProvider tablePropertiesProvider,
                                  StateStoreProvider stateStoreProvider,
                                  Configuration configuration) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.configuration = configuration;
    }

    public void run() throws StateStoreException, IOException {
        List<TableProperties> tables = tablePropertiesProvider.streamAllTables()
                .collect(Collectors.toUnmodifiableList());
        LOGGER.info("Found {} tables", tables.size());
        for (TableProperties table : tables) {
            StateStore stateStore = stateStoreProvider.getStateStore(table);
            List<SplitPartitionJobDefinition> splitPartitionJobs = new ArrayList<>();
            new FindPartitionsToSplit(instanceProperties, table, stateStore, splitPartitionJobs::add).run();
            LOGGER.info("Found {} partitions to be split in table {}", splitPartitionJobs.size(), table.getId());
            SplitPartition splitPartition = new SplitPartition(stateStore, table.getSchema(), configuration);
            for (SplitPartitionJobDefinition job : splitPartitionJobs) {
                LOGGER.info("Splitting partition {}", job.getPartition());
                splitPartition.splitPartition(job.getPartition(), job.getFileNames());
            }
        }
    }

    public static void main(String[] args) throws StateStoreException, IOException {
        if (args.length != 1) {
            System.out.println("Usage: <instance-id>");
            return;
        }
        String instanceId = args[0];
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        AmazonSQS sqsClient = buildAwsV1Client(AmazonSQSClientBuilder.standard());
        try {
            InstanceProperties instanceProperties = new InstanceProperties();
            instanceProperties.loadFromS3GivenInstanceId(s3Client, instanceId);
            Configuration configuration = getConfigurationForClient();
            TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient);
            StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, configuration);
            new FindAndSplitPartitions(instanceProperties, tablePropertiesProvider, stateStoreProvider, configuration).run();
        } finally {
            s3Client.shutdown();
            dynamoDBClient.shutdown();
            sqsClient.shutdown();
        }
    }
}
