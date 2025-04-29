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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AwsDrainSqsQueueTest {

    PriorityBlockingQueue<Message> queue = new PriorityBlockingQueue<>(10, Comparator.comparing(Message::messageId));
    ReceiveMessages receiveMessages = receiveFromQueue();

    @Nested
    @DisplayName("Drain queue")
    class DrainQueue {

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
    }

    @Nested
    @DisplayName("Drain queue with expected number of messages")
    class DrainQueueWithExpectedNumberOfMessages {

        @Test
        void shouldRetryAfterEmptyReceiveWhenExpectingANumberOfMessages() {
            // Given we have two messages
            addMessages("A", "B");
            // And we fake the behaviour of SQS to receive a message, an empty response, another message
            receiveMessages = receiveActions(receiveFromQueue(), receiveNoMessages(), receiveFromQueue(), receiveNoMessages());

            // When
            Set<Message> messages = drainQueueOneMessageAtATime()
                    .drainExpectingMessagesWithRetriesWhenEmpty(2, 1, "queue").collect(toSet());

            // Then
            assertThat(messages).isEqualTo(
                    streamMessages("A", "B").collect(toSet()));
        }

        @Test
        void shouldStopRetryingAfterEmptyReceiveWithConfiguredLimit() {
            // Given we have two messages
            addMessages("A", "B");
            // And we fake the behaviour of SQS to receive one message, followed by multiple empty responses
            receiveMessages = receiveActions(receiveFromQueue(), receiveNoMessages(), receiveNoMessages());

            // When / Then
            Assertions.setMaxStackTraceElementsDisplayed(200);
            AwsDrainSqsQueue drainSqsQueue = drainQueueOneMessageAtATime();
            assertThatThrownBy(() -> drainSqsQueue.drainExpectingMessagesWithRetriesWhenEmpty(2, 1, "queue").toList())
                    .isInstanceOf(RetriesLimitHitException.class)
                    .hasMessage("Found 2 empty receives with maximum of 1 retries");
        }

        @Test
        void shouldResetEmptyReceiveRetriesWhenMessageIsReceived() {
            // Given we have messages
            addMessages("A", "B", "C");
            // And we fake the behaviour of SQS to have two separate empty receive retries
            receiveMessages = receiveActions(
                    receiveFromQueue(), receiveNoMessages(),
                    receiveFromQueue(), receiveNoMessages(),
                    receiveFromQueue(), receiveNoMessages());

            // When
            Set<Message> messages = drainQueueOneMessageAtATime()
                    .drainExpectingMessagesWithRetriesWhenEmpty(3, 1, "queue").collect(toSet());

            // Then
            assertThat(messages).isEqualTo(
                    streamMessages("A", "B", "C").collect(toSet()));
        }

        @Test
        void shouldStopRetryingAfterOnlyEmptyReceives() {
            // Given we fake the behaviour of SQS to only return empty responses
            receiveMessages = receiveActions(receiveNoMessages(), receiveNoMessages());

            // When / Then
            Assertions.setMaxStackTraceElementsDisplayed(200);
            AwsDrainSqsQueue drainSqsQueue = drainQueueOneMessageAtATime();
            assertThatThrownBy(() -> drainSqsQueue.drainExpectingMessagesWithRetriesWhenEmpty(2, 1, "queue").toList())
                    .isInstanceOf(RetriesLimitHitException.class)
                    .hasMessage("Found 2 empty receives with maximum of 1 retries");
        }

        @Test
        void shouldReceiveMessageAfterStartingWithEmptyReceive() {
            // Given
            receiveMessages = receiveActions(receiveNoMessages(), receiveFromQueue(), receiveNoMessages());
            addMessages("A");

            // When
            Set<Message> messages = drainQueueOneMessageAtATime()
                    .drainExpectingMessagesWithRetriesWhenEmpty(1, 1, "queue").collect(toSet());

            // Then
            assertThat(messages).isEqualTo(
                    streamMessages("A").collect(toSet()));
        }
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
                .receiveMessages(receiveMessages);
    }

    private AwsDrainSqsQueue drainQueueOneMessageAtATime() {
        return drainSqsQueueBuilder()
                .numThreads(1)
                .messagesPerBatchPerThread(1)
                .messagesPerReceive(1)
                .build();
    }

    private ReceiveMessages receiveFromQueue() {
        return (queueUrl, maxNumberOfMessages, waitTimeSeconds) -> {
            List<Message> messages = new ArrayList<>(maxNumberOfMessages);
            queue.drainTo(messages, maxNumberOfMessages);
            return messages;
        };
    }

    private ReceiveMessages receiveNoMessages() {
        return (queueUrl, maxNumberOfMessages, waitTimeSeconds) -> List.of();
    }

    private ReceiveMessages receiveActions(ReceiveMessages... actions) {
        Iterator<ReceiveMessages> iterator = List.of(actions).iterator();
        return (queueUrl, maxNumberOfMessages, waitTimeSeconds) -> {
            ReceiveMessages action = iterator.next();
            return action.receiveAndDeleteMessages(queueUrl, maxNumberOfMessages, waitTimeSeconds);
        };
    }

}
