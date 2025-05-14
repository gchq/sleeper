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
package sleeper.compaction.job.creationv2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequestSerDe;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher.DeleteBatch;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher.ReadBatch;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher.ReturnRequestToPendingQueue;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher.SendDeadLetter;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher.SendJobs;
import sleeper.compaction.tracker.job.CompactionJobTrackerFactory;
import sleeper.configurationv2.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.statestorev2.StateStoreFactory;

import java.time.Instant;
import java.util.function.Supplier;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_QUEUE_URL;

public class AwsCompactionJobDispatcher {

    private AwsCompactionJobDispatcher() {
    }

    public static final Logger LOGGER = LoggerFactory.getLogger(AwsCompactionJobDispatcher.class);

    public static CompactionJobDispatcher from(
            S3Client s3, DynamoDbClient dynamoDB, SqsClient sqs, S3TransferManager conf, InstanceProperties instanceProperties, Supplier<Instant> timeSupplier) {
        CompactionJobSerDe compactionJobSerDe = new CompactionJobSerDe();
        return new CompactionJobDispatcher(instanceProperties,
                S3TableProperties.createProvider(instanceProperties, s3, dynamoDB),
                StateStoreFactory.createProvider(instanceProperties, s3, dynamoDB, conf),
                CompactionJobTrackerFactory.getTracker(dynamoDB, instanceProperties),
                readBatch(s3, compactionJobSerDe),
                sendJobs(instanceProperties, sqs, compactionJobSerDe), 10,
                deleteBatch(s3),
                returnToQueue(instanceProperties, sqs), sendDeadLetter(instanceProperties, sqs), timeSupplier);
    }

    private static SendJobs sendJobs(InstanceProperties instanceProperties, SqsClient sqs, CompactionJobSerDe compactionJobSerDe) {
        return jobs -> {
            SendMessageBatchResponse response = sqs.sendMessageBatch(SendMessageBatchRequest.builder()
                    .queueUrl(instanceProperties.get(COMPACTION_JOB_QUEUE_URL))
                    .entries(jobs.stream()
                            .map(job -> SendMessageBatchRequestEntry.builder()
                                    .id(job.getId())
                                    .messageBody(compactionJobSerDe.toJson(job))
                                    .build())
                            .toList())
                    .build());
            if (!response.failed().isEmpty()) {
                LOGGER.error("Found failures sending jobs: {}", response.failed());
                throw new RuntimeException("Failed sending halfway through batch");
            }
        };
    }

    private static ReadBatch readBatch(S3Client s3, CompactionJobSerDe compactionJobSerDe) {
        return (bucketName, key) -> compactionJobSerDe.batchFromJson(s3.getObject(GetObjectRequest.builder()
                .bucket(bucketName).key(key).build()).toString());
    }

    private static DeleteBatch deleteBatch(S3Client s3) {
        return (bucketName, key) -> {
            s3.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucketName).key(key).build());
        };
    }

    private static ReturnRequestToPendingQueue returnToQueue(InstanceProperties instanceProperties, SqsClient sqs) {
        CompactionJobDispatchRequestSerDe serDe = new CompactionJobDispatchRequestSerDe();
        return (request, delaySeconds) -> sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(instanceProperties.get(COMPACTION_PENDING_QUEUE_URL))
                .messageBody(serDe.toJson(request))
                .delaySeconds(delaySeconds)
                .build());
    }

    private static SendDeadLetter sendDeadLetter(InstanceProperties instanceProperties, SqsClient sqs) {
        CompactionJobDispatchRequestSerDe serDe = new CompactionJobDispatchRequestSerDe();
        return request -> sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(instanceProperties.get(COMPACTION_PENDING_DLQ_URL))
                .messageBody(serDe.toJson(request))
                .build());
    }

}
