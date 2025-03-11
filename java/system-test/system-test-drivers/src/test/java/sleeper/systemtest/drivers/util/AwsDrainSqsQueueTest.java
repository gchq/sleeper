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

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

class AwsDrainSqsQueueTest {

    PriorityBlockingQueue<Message> queue = new PriorityBlockingQueue<>(10, Comparator.comparing(Message::messageId));

    @Test
    void shouldReceiveMessagesOnOneThread() {
        // Given
        addMessages("A", "B", "C", "D", "E");
        AwsDrainSqsQueue drainSqsQueue = drainSqsQueueBuilder()
                .numThreads(1)
                .messagesPerBatchPerThread(3)
                .messagesPerReceive(2)
                .build();

        // When
        Set<Message> messages = drainSqsQueue.drain("queue").collect(toSet());

        // Then
        assertThat(messages).isEqualTo(
                streamMessages("A", "B", "C", "D", "E").collect(toSet()));
    }

    @Test
    void shouldReceiveMessagesAcrossThreads() {
        // Given
        addMessages("A", "B", "C", "D", "E");
        AwsDrainSqsQueue drainSqsQueue = drainSqsQueueBuilder()
                .numThreads(2)
                .messagesPerBatchPerThread(2)
                .messagesPerReceive(1)
                .build();

        // When
        Set<Message> messages = drainSqsQueue.drain("queue").collect(toSet());

        // Then
        assertThat(messages).isEqualTo(
                streamMessages("A", "B", "C", "D", "E").collect(toSet()));
    }

    private void addMessages(String... ids) {
        queue.addAll(streamMessages(ids).toList());
    }

    private Stream<Message> streamMessages(String... ids) {
        return Stream.of(ids).map(this::message);
    }

    private Message message(String id) {
        return Message.builder()
                .messageId(id)
                .receiptHandle(id)
                .body(id)
                .build();
    }

    private AwsDrainSqsQueue.Builder drainSqsQueueBuilder() {
        return AwsDrainSqsQueue.builder()
                .receiveMessages((queueUrl, maxNumberOfMessages, waitTimeSeconds) -> {
                    List<Message> messages = new ArrayList<>(maxNumberOfMessages);
                    queue.drainTo(messages, maxNumberOfMessages);
                    return messages;
                });
    }

}
