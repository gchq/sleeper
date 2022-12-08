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
package sleeper.ingest.job;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.iterator.IteratorException;
import sleeper.ingest.IngestResult;
import sleeper.ingest.task.IngestTaskFinishedStatus;
import sleeper.ingest.task.IngestTaskStatus;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.ingest.task.status.DynamoDBIngestTaskStatusStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import sleeper.utils.HadoopConfigurationProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;

public class IngestJobQueueConsumerRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestJobQueueConsumerRunner.class);

    private final IngestJobSource jobSource;
    private final String taskId;
    private final IngestTaskStatusStore statusStore;
    private final Supplier<Instant> getTimeNow;
    private final IngestJobHandler runJobCallback;

    public IngestJobQueueConsumerRunner(
            IngestJobSource jobSource, String taskId, IngestTaskStatusStore statusStore,
            IngestJobHandler runJobCallback, Supplier<Instant> getTimeNow) {
        this.getTimeNow = getTimeNow;
        this.jobSource = jobSource;
        this.taskId = taskId;
        this.statusStore = statusStore;
        this.runJobCallback = runJobCallback;
    }

    public IngestJobQueueConsumerRunner(
            IngestJobQueueConsumer queueConsumer, String taskId, IngestTaskStatusStore statusStore, IngestJobHandler runJobCallback) {
        this(queueConsumer, taskId, statusStore, runJobCallback, Instant::now);
    }

    public void run() throws IOException, IteratorException, StateStoreException {
        Instant startTaskTime = getTimeNow.get();
        IngestTaskStatus.Builder taskStatusBuilder = IngestTaskStatus.builder().taskId(taskId).startTime(startTaskTime);
        statusStore.taskStarted(taskStatusBuilder.build());
        LOGGER.info("IngestTask started at = {}", startTaskTime);

        IngestTaskFinishedStatus.Builder taskFinishedStatusBuilder = IngestTaskFinishedStatus.builder();
        jobSource.consumeJobs(job -> {
            Instant startTime = getTimeNow.get();
            IngestResult result = runJobCallback.ingest(job);
            Instant finishTime = getTimeNow.get();
            taskFinishedStatusBuilder.addIngestResult(result, startTime, finishTime);
            return result;
        });

        Instant finishTaskTime = getTimeNow.get();
        taskStatusBuilder.finishedStatus(taskFinishedStatusBuilder
                .finish(startTaskTime, finishTaskTime).build());
        statusStore.taskFinished(taskStatusBuilder.build());
        LOGGER.info("IngestTask finished at = {}", finishTaskTime);
    }

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
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
        IngestTaskStatusStore taskStore = DynamoDBIngestTaskStatusStore.from(dynamoDBClient, instanceProperties);
        String taskId = UUID.randomUUID().toString();
        IngestJobRunner ingestJobRunner = new IngestJobRunner(
                objectFactory,
                instanceProperties,
                tablePropertiesProvider,
                stateStoreProvider,
                localDir,
                S3AsyncClient.create(),
                HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
        IngestJobQueueConsumer queueConsumer = new IngestJobQueueConsumer(sqsClient, cloudWatchClient, instanceProperties);
        IngestJobQueueConsumerRunner ingestJobQueueConsumerRunner = new IngestJobQueueConsumerRunner(
                queueConsumer, taskId, taskStore, ingestJobRunner::ingest);
        ingestJobQueueConsumerRunner.run();

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
}
