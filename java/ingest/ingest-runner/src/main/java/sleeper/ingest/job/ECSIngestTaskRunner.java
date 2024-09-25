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
package sleeper.ingest.job;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.s3properties.PropertiesReloader;
import sleeper.configuration.s3properties.S3InstanceProperties;
import sleeper.configuration.statestore.StateStoreProvider;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.util.LoggedDuration;
import sleeper.ingest.impl.partitionfilewriter.AsyncS3PartitionFileWriterFactory;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.ingest.status.store.task.IngestTaskStatusStoreFactory;
import sleeper.ingest.task.IngestTask;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreFactory;

import java.time.Instant;
import java.util.UUID;

import static sleeper.configuration.properties.instance.IngestProperty.S3A_INPUT_FADVISE;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

/**
 * Runs an ingest task in ECS. Delegates the running of ingest jobs to {@link IngestJobRunner},
 * and the processing of SQS messages to {@link IngestJobQueueConsumer}.
 */
public class ECSIngestTaskRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ECSIngestTaskRunner.class);

    private ECSIngestTaskRunner() {
    }

    public static void main(String[] args) throws ObjectFactoryException {
        if (1 != args.length) {
            System.err.println("Error: must have 1 argument (config bucket), got " + args.length + " arguments (" + StringUtils.join(args, ',') + ")");
            System.exit(1);
        }
        String s3Bucket = args[0];

        Instant startTime = Instant.now();
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        AmazonSQS sqsClient = buildAwsV1Client(AmazonSQSClientBuilder.standard());
        AmazonCloudWatch cloudWatchClient = buildAwsV1Client(AmazonCloudWatchClientBuilder.standard());
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());

        try {
            InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, s3Bucket);

            ObjectFactory objectFactory = new ObjectFactory(instanceProperties, s3Client, "/tmp");
            String localDir = "/mnt/scratch";
            String taskId = UUID.randomUUID().toString();

            IngestTask ingestTask = createIngestTask(objectFactory, instanceProperties, localDir,
                    taskId, s3Client, dynamoDBClient, sqsClient, cloudWatchClient,
                    AsyncS3PartitionFileWriterFactory.s3AsyncClientFromProperties(instanceProperties),
                    ingestHadoopConfiguration(instanceProperties));
            ingestTask.run();
        } finally {
            s3Client.shutdown();
            LOGGER.info("Shut down s3Client");
            sqsClient.shutdown();
            LOGGER.info("Shut down sqsClient");
            dynamoDBClient.shutdown();
            LOGGER.info("Shut down dynamoDBClient");
            cloudWatchClient.shutdown();
            LOGGER.info("Shut down cloudWatchClient");
            LOGGER.info("Total run time = {}", LoggedDuration.withFullOutput(startTime, Instant.now()));
        }
    }

    public static IngestTask createIngestTask(
            ObjectFactory objectFactory, InstanceProperties instanceProperties, String localDir, String taskId,
            AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient, AmazonSQS sqsClient, AmazonCloudWatch cloudWatchClient,
            S3AsyncClient s3AsyncClient, Configuration hadoopConfiguration) {
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient);
        StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoDBClient, hadoopConfiguration);
        IngestTaskStatusStore taskStore = IngestTaskStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);
        IngestJobStatusStore jobStore = IngestJobStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);
        PropertiesReloader propertiesReloader = PropertiesReloader.ifConfigured(
                s3Client, instanceProperties, tablePropertiesProvider);
        IngestJobRunner ingestJobRunner = new IngestJobRunner(objectFactory, instanceProperties, tablePropertiesProvider,
                propertiesReloader, stateStoreProvider, jobStore, taskId, localDir,
                s3Client, s3AsyncClient, sqsClient, hadoopConfiguration, Instant::now);
        IngestJobQueueConsumer queueConsumer = new IngestJobQueueConsumer(
                sqsClient, cloudWatchClient, instanceProperties, hadoopConfiguration,
                new DynamoDBTableIndex(instanceProperties, dynamoDBClient), jobStore);
        return new IngestTask(() -> UUID.randomUUID().toString(), Instant::now,
                queueConsumer, ingestJobRunner, jobStore, taskStore, taskId);
    }

    private static Configuration ingestHadoopConfiguration(InstanceProperties instanceProperties) {
        Configuration conf = HadoopConfigurationProvider.getConfigurationForECS(instanceProperties);
        conf.set("fs.s3a.experimental.input.fadvise", instanceProperties.get(S3A_INPUT_FADVISE));
        return conf;
    }
}
