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
package sleeper.systemtest.compaction;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.util.PollWithRetries;
import sleeper.configuration.properties.InstanceProperties;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.PARTITION_SPLITTING_QUEUE_URL;

public class WaitForPartitionSplittingQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForPartitionSplittingQueue.class);
    private static final long POLL_INTERVAL_MILLIS = 5000;
    private static final int MAX_POLLS = 12;

    private final AmazonSQS sqsClient;
    private final InstanceProperties instanceProperties;
    private final PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(POLL_INTERVAL_MILLIS, MAX_POLLS);

    public WaitForPartitionSplittingQueue(AmazonSQS sqsClient, InstanceProperties instanceProperties) {
        this.sqsClient = sqsClient;
        this.instanceProperties = instanceProperties;
    }

    public void pollUntilFinished() throws InterruptedException {
        poll.pollUntil("partition splits finished", this::isPartitionSplitsFinished);
    }

    private boolean isPartitionSplitsFinished() {
        int unconsumedMessages = getUnconsumedPartitionSplitMessages();
        LOGGER.info("Unfinished partition splits: {}", unconsumedMessages);
        return unconsumedMessages == 0;
    }

    private int getUnconsumedPartitionSplitMessages() {
        GetQueueAttributesResult result = sqsClient.getQueueAttributes(new GetQueueAttributesRequest()
                .withQueueUrl(instanceProperties.get(PARTITION_SPLITTING_QUEUE_URL))
                .withAttributeNames(
                        QueueAttributeName.ApproximateNumberOfMessages,
                        QueueAttributeName.ApproximateNumberOfMessagesNotVisible));
        return getInt(result, QueueAttributeName.ApproximateNumberOfMessages) +
                getInt(result, QueueAttributeName.ApproximateNumberOfMessagesNotVisible);
    }

    private static int getInt(GetQueueAttributesResult result, QueueAttributeName attribute) {
        return Integer.parseInt(result.getAttributes().get(attribute.toString()));
    }
}
