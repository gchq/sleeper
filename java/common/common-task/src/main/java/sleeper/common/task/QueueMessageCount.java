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

package sleeper.common.task;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.util.Map;
import java.util.Objects;

/**
 * Retrieves estimates of the number of messages on an SQS queue. Note that these are updated slowly and asynchronously.
 * They should be considered a rough snapshot of the state several minutes ago.
 */
public class QueueMessageCount {
    private final int approximateNumberOfMessages;
    private final int approximateNumberOfMessagesNotVisible;

    private QueueMessageCount(int approximateNumberOfMessages, int approximateNumberOfMessagesNotVisible) {
        this.approximateNumberOfMessages = approximateNumberOfMessages;
        this.approximateNumberOfMessagesNotVisible = approximateNumberOfMessagesNotVisible;
    }

    public static QueueMessageCount approximateNumberVisibleAndNotVisible(int visible, int notVisible) {
        return new QueueMessageCount(visible, notVisible);
    }

    public static Client withSqsClient(SqsClient sqsClient) {
        return sqsQueueUrl -> getQueueMessageCountFromSqs(sqsQueueUrl, sqsClient);
    }

    @FunctionalInterface
    public interface Client {
        QueueMessageCount getQueueMessageCount(String queueUrl);
    }

    private static QueueMessageCount getQueueMessageCountFromSqs(String sqsJobQueueUrl, SqsClient sqsClient) {
        Map<QueueAttributeName, String> attributes = sqsClient.getQueueAttributes(request -> request.queueUrl(sqsJobQueueUrl)
                .attributeNames(
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE))
                .attributes();
        int approximateNumberOfMessages = Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES));
        int approximateNumberOfMessagesNotVisible = Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE));
        return approximateNumberVisibleAndNotVisible(approximateNumberOfMessages, approximateNumberOfMessagesNotVisible);
    }

    public int getApproximateNumberOfMessages() {
        return approximateNumberOfMessages;
    }

    public int getApproximateNumberOfMessagesNotVisible() {
        return approximateNumberOfMessagesNotVisible;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueueMessageCount that = (QueueMessageCount) o;
        return approximateNumberOfMessages == that.approximateNumberOfMessages && approximateNumberOfMessagesNotVisible == that.approximateNumberOfMessagesNotVisible;
    }

    @Override
    public int hashCode() {
        return Objects.hash(approximateNumberOfMessages, approximateNumberOfMessagesNotVisible);
    }

    @Override
    public String toString() {
        return Map.of(
                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES.toString(), getApproximateNumberOfMessages(),
                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE.toString(), getApproximateNumberOfMessagesNotVisible()).toString();
    }
}
