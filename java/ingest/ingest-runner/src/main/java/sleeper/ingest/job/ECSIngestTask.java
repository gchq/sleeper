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
package sleeper.ingest.job;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.PropertiesReloader;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.iterator.IteratorException;
import sleeper.ingest.impl.partitionfilewriter.AsyncS3PartitionFileWriterFactory;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.ingest.status.store.task.IngestTaskStatusStoreFactory;
import sleeper.ingest.task.IngestTask;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import sleeper.utils.HadoopConfigurationProvider;

import java.io.IOException;
import java.util.UUID;

import static sleeper.configuration.properties.instance.IngestProperty.S3A_INPUT_FADVISE;

public class ECSIngestTask {
    private ECSIngestTask() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ECSIngestTask.class);

    public static void main(String[] args) throws IOException, StateStoreException, IteratorException, ObjectFactoryException {
        if (1 != args.length) {
            System.err.println("Error: must have 1 argument (s3Bucket)");
            System.exit(1);
        }

        long startTime = System.currentTimeMillis();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        AmazonCloudWatch cloudWatchClient = AmazonCloudWatchClientBuilder.defaultClient();
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

        String s3Bucket = args[0];
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, s3Bucket);

        ObjectFactory objectFactory = new ObjectFactory(instanceProperties, s3Client, "/tmp");
        String localDir = "/mnt/scratch";
        String taskId = UUID.randomUUID().toString();

        IngestTask ingestTask = createIngestTask(objectFactory, instanceProperties, localDir,
                taskId, s3Client, dynamoDBClient, sqsClient, cloudWatchClient,
                AsyncS3PartitionFileWriterFactory.s3AsyncClientFromProperties(instanceProperties),
                ingestHadoopConfiguration(instanceProperties));
        ingestTask.run();

        s3Client.shutdown();
        LOGGER.info("Shut down s3Client");
        sqsClient.shutdown();
        LOGGER.info("Shut down sqsClient");
        dynamoDBClient.shutdown();
        LOGGER.info("Shut down dynamoDBClient");
        long finishTime = System.currentTimeMillis();
        double runTimeInSeconds = (finishTime - startTime) / 1000.0;
        LOGGER.info("IngestFromIngestJobsQueueRunner total run time = {}", runTimeInSeconds);
    }

    public static IngestTask createIngestTask(ObjectFactory objectFactory,
                                              InstanceProperties instanceProperties,
                                              String localDir,
                                              String taskId,
                                              AmazonS3 s3Client,
                                              AmazonDynamoDB dynamoDBClient,
                                              AmazonSQS sqsClient,
                                              AmazonCloudWatch cloudWatchClient,
                                              S3AsyncClient s3AsyncClient,
                                              Configuration hadoopConfiguration) {
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, hadoopConfiguration);
        IngestTaskStatusStore taskStore = IngestTaskStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);
        IngestJobStatusStore jobStore = IngestJobStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);
        PropertiesReloader propertiesReloader = PropertiesReloader.ifConfigured(
                s3Client, instanceProperties, tablePropertiesProvider);
        IngestJobRunner ingestJobRunner = new IngestJobRunner(
                objectFactory,
                instanceProperties,
                tablePropertiesProvider,
                propertiesReloader,
                stateStoreProvider,
                localDir,
                s3AsyncClient,
                hadoopConfiguration);
        IngestJobQueueConsumer queueConsumer = new IngestJobQueueConsumer(
                sqsClient, cloudWatchClient, instanceProperties, hadoopConfiguration, jobStore);
        return new IngestTask(
                queueConsumer, taskId, taskStore, jobStore, ingestJobRunner);
    }

    private static Configuration ingestHadoopConfiguration(InstanceProperties instanceProperties) {
        Configuration conf = HadoopConfigurationProvider.getConfigurationForECS(instanceProperties);
        conf.set("fs.s3a.experimental.input.fadvise", instanceProperties.get(S3A_INPUT_FADVISE));
        return conf;
    }
}
