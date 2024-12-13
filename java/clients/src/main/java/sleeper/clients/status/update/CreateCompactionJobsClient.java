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

package sleeper.clients.status.update;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.hadoop.conf.Configuration;

import sleeper.compaction.core.job.CompactionJobStatusStore;
import sleeper.compaction.core.job.creation.CreateCompactionJobs;
import sleeper.compaction.job.creation.AwsCreateCompactionJobs;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.configuration.jars.S3UserJarsLoader;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.util.ObjectFactoryException;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

/**
 * Command line client to create compaction jobs to be run for specified Sleeper tables.
 */
public class CreateCompactionJobsClient {
    private CreateCompactionJobsClient() {
    }

    private static final Map<String, CreateJobsMode> ARG_TO_MODE = Map.of(
            "default", CreateCompactionJobs::createJobsWithStrategy,
            "all", CreateCompactionJobs::createJobWithForceAllFiles);

    @FunctionalInterface
    public interface CreateJobsMode {
        void createJobs(CreateCompactionJobs creator, TableProperties table) throws ObjectFactoryException, IOException;
    }

    public static void main(String[] args) throws ObjectFactoryException, IOException {
        if (args.length < 2) {
            System.out.println("Usage: <mode-all-or-default> <instance-id> <table-names-as-args>");
            return;
        }
        CreateJobsMode mode = ARG_TO_MODE.get(args[0].toLowerCase(Locale.ROOT));
        if (mode == null) {
            System.out.println("Supported modes for job creation are ALL or DEFAULT");
            return;
        }
        String instanceId = args[1];
        List<String> tableNames = Stream.of(args).skip(2).collect(toUnmodifiableList());
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        AmazonSQS sqsClient = buildAwsV1Client(AmazonSQSClientBuilder.standard());
        try {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDBClient);
            List<TableProperties> tables = tableNames.stream()
                    .map(name -> tablePropertiesProvider.getByName(name))
                    .collect(toUnmodifiableList());
            Configuration conf = HadoopConfigurationProvider.getConfigurationForClient(instanceProperties);
            StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoDBClient, conf);
            CompactionJobStatusStore jobStatusStore = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);
            CreateCompactionJobs jobCreator = AwsCreateCompactionJobs.from(
                    new S3UserJarsLoader(instanceProperties, s3Client, "/tmp").buildObjectFactory(),
                    instanceProperties, stateStoreProvider, jobStatusStore, s3Client, sqsClient);
            for (TableProperties table : tables) {
                mode.createJobs(jobCreator, table);
            }
        } finally {
            s3Client.shutdown();
            dynamoDBClient.shutdown();
            sqsClient.shutdown();
        }
    }
}
