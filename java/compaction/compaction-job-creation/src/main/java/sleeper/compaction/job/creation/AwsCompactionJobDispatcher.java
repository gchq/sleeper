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
package sleeper.compaction.job.creation;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequestSerDe;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher.ReadBatch;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher.ReturnRequestToPendingQueue;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher.SendDeadLetter;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher.SendJobs;
import sleeper.compaction.tracker.job.CompactionJobTrackerFactory;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.statestore.StateStoreFactory;

import java.time.Instant;
import java.util.function.Supplier;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_QUEUE_URL;

public class AwsCompactionJobDispatcher {

    public static final Logger LOGGER = LoggerFactory.getLogger(AwsCompactionJobDispatcher.class);

    public static CompactionJobDispatcher from(
            AmazonS3 s3, AmazonDynamoDB dynamoDB, AmazonSQS sqs, Configuration conf, InstanceProperties instanceProperties, Supplier<Instant> timeSupplier) {
        CompactionJobSerDe compactionJobSerDe = new CompactionJobSerDe();
        return new CompactionJobDispatcher(instanceProperties,
                S3TableProperties.createProvider(instanceProperties, s3, dynamoDB),
                StateStoreFactory.createProvider(instanceProperties, s3, dynamoDB, conf),
                CompactionJobTrackerFactory.getTracker(dynamoDB, instanceProperties),
                readBatch(s3, compactionJobSerDe), sendJobs(instanceProperties, sqs, compactionJobSerDe), 10,
                returnToQueue(instanceProperties, sqs), sendDeadLetter(instanceProperties, sqs), timeSupplier);
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
