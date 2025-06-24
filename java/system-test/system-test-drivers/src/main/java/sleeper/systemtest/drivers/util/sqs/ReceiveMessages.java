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
package sleeper.systemtest.drivers.util.sqs;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;

public interface ReceiveMessages {

    List<Message> receiveAndDeleteMessages(String queueUrl, int maxNumberOfMessages, int waitTimeSeconds);

    static ReceiveMessages from(SqsClient sqsClient) {
        return (queueUrl, maxNumberOfMessages, waitTimeSeconds) -> sqsReceiveAndDeleteMessages(
                sqsClient, queueUrl, maxNumberOfMessages, waitTimeSeconds);
    }

    private static List<Message> sqsReceiveAndDeleteMessages(SqsClient sqsClient, String queueUrl, int maxNumberOfMessages, int waitTimeSeconds) {
        List<Message> messages = sqsClient.receiveMessage(request -> request
                .queueUrl(queueUrl)
                .maxNumberOfMessages(maxNumberOfMessages)
                .waitTimeSeconds(waitTimeSeconds))
                .messages();
        if (messages.isEmpty()) {
            return List.of();
        }
        DeleteMessageBatchResponse deleteResult = sqsClient.deleteMessageBatch(request -> request
                .queueUrl(queueUrl)
                .entries(messages.stream()
                        .map(message -> DeleteMessageBatchRequestEntry.builder()
                                .id(message.messageId())
                                .receiptHandle(message.receiptHandle())
                                .build())
                        .toList()));
        if (!deleteResult.failed().isEmpty()) {
            throw new RuntimeException("Failed deleting messages: " + deleteResult.failed());
        }
        return messages;
    }
}
