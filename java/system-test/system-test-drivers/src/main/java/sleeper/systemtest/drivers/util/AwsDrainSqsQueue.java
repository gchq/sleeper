/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.systemtest.drivers.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.util.List;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.counting;

public class AwsDrainSqsQueue {
    public static final Logger LOGGER = LoggerFactory.getLogger(AwsDrainSqsQueue.class);

    private AwsDrainSqsQueue() {
    }

    public static void emptyQueueForWholeInstance(SqsClient sqs, String queueUrl) {
        LOGGER.info("Draining queue until empty: {}", queueUrl);
        long messages = drainQueueForWholeInstance(sqs, queueUrl).collect(counting());
        LOGGER.info("Deleted {} messages", messages);
    }

    public static Stream<Message> drainQueueForWholeInstance(SqsClient sqs, String queueUrl) {
        return Stream.iterate(
                receiveMessages(sqs, queueUrl), not(List::isEmpty), lastJobs -> receiveMessages(sqs, queueUrl))
                .flatMap(List::stream);
    }

    private static List<Message> receiveMessages(SqsClient sqs, String queueUrl) {
        ReceiveMessageResponse receiveResult = sqs.receiveMessage(request -> request
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(5));
        List<Message> messages = receiveResult.messages();
        if (messages.isEmpty()) {
            return List.of();
        }
        DeleteMessageBatchResponse deleteResult = sqs.deleteMessageBatch(request -> request
                .queueUrl(queueUrl)
                .entries(messages.stream()
                        .map(message -> DeleteMessageBatchRequestEntry.builder()
                                .id(message.messageId())
                                .receiptHandle(message.receiptHandle())
                                .build())
                        .toList()));
        if (!deleteResult.failed().isEmpty()) {
            throw new RuntimeException("Failed deleting compaction job messages: " + deleteResult.failed());
        }
        return messages;
    }

}
