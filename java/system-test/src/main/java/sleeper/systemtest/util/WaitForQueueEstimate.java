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

package sleeper.systemtest.util;

import com.amazonaws.services.sqs.AmazonSQS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.util.PollWithRetries;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.InstanceProperty;
import sleeper.job.common.QueueMessageCount;

import java.util.function.Predicate;

import static sleeper.job.common.QueueMessageCount.withSqsClient;

public class WaitForQueueEstimate {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForQueueEstimate.class);

    private final QueueMessageCount.Client queueClient;
    private final Predicate<QueueMessageCount> isFinished;
    private final String description;
    private final String queueUrl;

    public static WaitForQueueEstimate notEmpty(
            QueueMessageCount.Client queueClient, InstanceProperties instanceProperties, InstanceProperty queueProperty) {
        String queueUrl = instanceProperties.get(queueProperty);
        return new WaitForQueueEstimate(queueClient, queueUrl,
                estimate -> estimate.getApproximateNumberOfMessages() > 0,
                "estimate not empty for queue " + queueUrl);
    }

    public static WaitForQueueEstimate isEmpty(
            AmazonSQS sqsClient, InstanceProperties instanceProperties, InstanceProperty queueProperty) {
        String queueUrl = instanceProperties.get(queueProperty);
        return new WaitForQueueEstimate(withSqsClient(sqsClient), queueUrl,
                estimate -> estimate.getApproximateNumberOfMessages() == 0,
                "estimate empty for queue " + queueUrl);
    }

    public static WaitForQueueEstimate containsUnfinishedJobs(
            AmazonSQS sqsClient, InstanceProperties instanceProperties, InstanceProperty queueProperty,
            CompactionJobStatusStore statusStore, String tableName) {
        int unfinished = statusStore.getUnfinishedJobs(tableName).size();
        LOGGER.info("Found {} unfinished compaction jobs", unfinished);
        return new WaitForQueueEstimate(withSqsClient(sqsClient), instanceProperties, queueProperty,
                estimate -> estimate.getApproximateNumberOfMessages() >= unfinished,
                "queue estimate matching unfinished compaction jobs");
    }

    private WaitForQueueEstimate(QueueMessageCount.Client queueClient,
                                 InstanceProperties properties, InstanceProperty queueProperty,
                                 Predicate<QueueMessageCount> isFinished, String description) {
        this(queueClient, properties.get(queueProperty), isFinished, description);
    }

    private WaitForQueueEstimate(QueueMessageCount.Client queueClient, String queueUrl,
                                 Predicate<QueueMessageCount> isFinished, String description) {
        this.queueClient = queueClient;
        this.queueUrl = queueUrl;
        this.isFinished = isFinished;
        this.description = description;
    }

    public void pollUntilFinished(PollWithRetries poll) throws InterruptedException {
        LOGGER.info("Waiting until {}", description);
        poll.pollUntil(description, this::isFinished);
    }

    private boolean isFinished() {
        QueueMessageCount count = queueClient.getQueueMessageCount(queueUrl);
        LOGGER.info("Message count: {}", count);
        return isFinished.test(count);
    }
}
