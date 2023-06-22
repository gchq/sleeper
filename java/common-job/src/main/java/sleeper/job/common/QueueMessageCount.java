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

package sleeper.job.common;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.QueueAttributeName;

import java.util.Map;
import java.util.Objects;

import static com.amazonaws.services.sqs.model.QueueAttributeName.ApproximateNumberOfMessages;
import static com.amazonaws.services.sqs.model.QueueAttributeName.ApproximateNumberOfMessagesNotVisible;

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

    public static Client withSqsClient(AmazonSQS sqsClient) {
        return sqsQueueUrl -> getQueueMessageCountFromSqs(sqsQueueUrl, sqsClient);
    }

    @FunctionalInterface
    public interface Client {
        QueueMessageCount getQueueMessageCount(String queueUrl);
    }

    private static QueueMessageCount getQueueMessageCountFromSqs(String sqsJobQueueUrl, AmazonSQS sqsClient) {
        GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest()
                .withQueueUrl(sqsJobQueueUrl)
                .withAttributeNames(QueueAttributeName.ApproximateNumberOfMessages,
                        QueueAttributeName.ApproximateNumberOfMessagesNotVisible);
        GetQueueAttributesResult sizeResult = sqsClient.getQueueAttributes(getQueueAttributesRequest);
        // See
        // https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_GetQueueAttributes.html
        int approximateNumberOfMessages = Integer.parseInt(sizeResult.getAttributes().get("ApproximateNumberOfMessages"));
        int approximateNumberOfMessagesNotVisible = Integer.parseInt(sizeResult.getAttributes().get("ApproximateNumberOfMessagesNotVisible"));
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
                ApproximateNumberOfMessages.toString(), getApproximateNumberOfMessages(),
                ApproximateNumberOfMessagesNotVisible.toString(), getApproximateNumberOfMessagesNotVisible()
        ).toString();
    }
}
