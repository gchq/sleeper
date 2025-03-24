/*
 * Copyright 2022-2025 Crown Copyright
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
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequest;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequestSerDe;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher.ReadBatch;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher.ReturnRequestToPendingQueue;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher.SendDeadLetter;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher.SendJobs;
import sleeper.compaction.tracker.job.CompactionJobTrackerFactory;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.util.ObjectFactoryException;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.time.Instant;

import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_QUEUE_URL;

public class DispatchCompactionJobsClient {
    public static final Logger LOGGER = LoggerFactory.getLogger(DispatchCompactionJobsClient.class);

    public static void main(String[] args) throws ObjectFactoryException, IOException {
        if (args.length < 1) {
            System.out.println("Usage: <instance-id>");
            return;
        }
        String instanceId = args[0];
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        AmazonSQS sqsClient = buildAwsV1Client(AmazonSQSClientBuilder.standard());
        try {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDBClient);
            Configuration conf = HadoopConfigurationProvider.getConfigurationForClient(instanceProperties);
            StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoDBClient, conf);
            CompactionJobTracker tracker = CompactionJobTrackerFactory.getTracker(dynamoDBClient, instanceProperties);
            CompactionJobSerDe compactionJobSerDe = new CompactionJobSerDe();
            CompactionJobDispatcher dispatcher = new CompactionJobDispatcher(
                    instanceProperties, tablePropertiesProvider, stateStoreProvider, tracker,
                    readBatch(s3Client, compactionJobSerDe), sendJobs(instanceProperties, sqsClient, compactionJobSerDe), 10,
                    returnToQueue(instanceProperties, sqsClient), sendDeadLetter(instanceProperties, sqsClient), Instant::now);
            CompactionJobDispatchRequestSerDe requestSerDe = new CompactionJobDispatchRequestSerDe();

            ReceiveMessageResult result = sqsClient.receiveMessage(instanceProperties.get(COMPACTION_PENDING_QUEUE_URL));
            for (Message message : result.getMessages()) {
                CompactionJobDispatchRequest request = requestSerDe.fromJson(message.getBody());
                dispatcher.dispatch(request);
            }
        } finally {
            s3Client.shutdown();
            dynamoDBClient.shutdown();
            sqsClient.shutdown();
        }
    }

    private static SendJobs sendJobs(InstanceProperties instanceProperties, AmazonSQS sqs, CompactionJobSerDe compactionJobSerDe) {
        return jobs -> {
            SendMessageBatchResult result = sqs.sendMessageBatch(new SendMessageBatchRequest()
                    .withQueueUrl(instanceProperties.get(COMPACTION_JOB_QUEUE_URL))
                    .withEntries(jobs.stream()
                            .map(job -> new SendMessageBatchRequestEntry(job.getId(), compactionJobSerDe.toJson(job)))
                            .toList()));
            if (!result.getFailed().isEmpty()) {
                LOGGER.error("Found failures sending jobs: {}", result.getFailed());
                throw new RuntimeException("Failed sending halfway through batch");
            }
        };
    }

    private static ReadBatch readBatch(AmazonS3 s3, CompactionJobSerDe compactionJobSerDe) {
        return (bucketName, key) -> compactionJobSerDe.batchFromJson(s3.getObjectAsString(bucketName, key));
    }

    private static ReturnRequestToPendingQueue returnToQueue(InstanceProperties instanceProperties, AmazonSQS sqs) {
        CompactionJobDispatchRequestSerDe serDe = new CompactionJobDispatchRequestSerDe();
        return (request, delaySeconds) -> sqs.sendMessage(new SendMessageRequest()
                .withQueueUrl(instanceProperties.get(COMPACTION_PENDING_QUEUE_URL))
                .withMessageBody(serDe.toJson(request))
                .withDelaySeconds(delaySeconds));
    }

    private static SendDeadLetter sendDeadLetter(InstanceProperties instanceProperties, AmazonSQS sqs) {
        CompactionJobDispatchRequestSerDe serDe = new CompactionJobDispatchRequestSerDe();
        return request -> sqs.sendMessage(new SendMessageRequest()
                .withQueueUrl(instanceProperties.get(COMPACTION_PENDING_DLQ_URL))
                .withMessageBody(serDe.toJson(request)));
    }

}