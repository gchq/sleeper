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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;

import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

/**
 * Initialises a state store with a single root partition.
 */
public class InitialiseStateStore {

    private InitialiseStateStore() {
    }

    /**
     * Initialises a state store with a single root partition from the command line.
     *
     * @param  args                the command line arguments
     * @throws StateStoreException if the state store initialisation fails
     */
    public static void main(String[] args) throws StateStoreException {
        if (2 != args.length) {
            System.out.println("Usage: <instance-id> <table-name>");
            return;
        }
        String instanceId = args[0];
        String tableName = args[1];

        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        try {
            InstanceProperties instanceProperties = new InstanceProperties();
            instanceProperties.loadFromS3GivenInstanceId(s3Client, instanceId);

            TableProperties tableProperties = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient).getByName(tableName);

            Configuration conf = HadoopConfigurationProvider.getConfigurationForClient();
            StateStore stateStore = new StateStoreFactory(instanceProperties, s3Client, dynamoDBClient, conf).getStateStore(tableProperties);

            stateStore.initialise();
        } finally {
            dynamoDBClient.shutdown();
            s3Client.shutdown();
        }
    }
}
