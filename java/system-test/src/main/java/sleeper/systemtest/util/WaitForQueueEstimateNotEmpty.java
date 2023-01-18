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
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.InstanceProperty;

public class WaitForQueueEstimateNotEmpty {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForQueueEstimateNotEmpty.class);
    private static final long POLL_INTERVAL_MILLIS = 5000;
    private static final int MAX_POLLS = 12;

    private final AmazonSQS sqsClient;
    private final String queueUrl;
    private final PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(POLL_INTERVAL_MILLIS, MAX_POLLS);

    public WaitForQueueEstimateNotEmpty(AmazonSQS sqsClient, InstanceProperties instanceProperties, InstanceProperty queueProperty) {
        this.sqsClient = sqsClient;
        this.queueUrl = instanceProperties.get(queueProperty);
    }

    public void pollUntilFinished() throws InterruptedException {
        LOGGER.info("Waiting for messages on {}", queueUrl);
        poll.pollUntil("queue estimated not empty", this::isEstimateNotEmpty);
    }

    private boolean isEstimateNotEmpty() {
        int approximateMessages = getApproximateMessages();
        LOGGER.info("Approximate number of messages: {}", approximateMessages);
        return approximateMessages > 0;
    }

    private int getApproximateMessages() {
        GetQueueAttributesResult result = sqsClient.getQueueAttributes(new GetQueueAttributesRequest()
                .withQueueUrl(queueUrl)
                .withAttributeNames(QueueAttributeName.ApproximateNumberOfMessages));
        return getInt(result, QueueAttributeName.ApproximateNumberOfMessages);
    }

    private static int getInt(GetQueueAttributesResult result, QueueAttributeName attribute) {
        return Integer.parseInt(result.getAttributes().get(attribute.toString()));
    }
}
