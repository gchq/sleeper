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
package sleeper.dynamodb.tools;

import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.util.PollWithRetries;
import sleeper.dynamodb.tools.DynamoDBRetryWithTimeout.DynamoRunner;
import sleeper.dynamodb.tools.DynamoDBRetryWithTimeout.ParameterSupplier;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DynamoDBRetryWithTimeoutTest {
    private final List<String> successfulMessages = new ArrayList<>();
    private final List<String> failedMessages = new ArrayList<>();

    @Nested
    @DisplayName("Idle timeout")
    class IdleTimeout {
        @Test
        void shouldNotTimeoutWhenMessageSucceedsMultipleTimes() throws Exception {
            // Given
            ParameterSupplier<Message> messages = messages(
                    success("m1"),
                    success("m2"),
                    success("m3"),
                    noMessage());
            Supplier<Instant> timeSupplier = List.of(
                    Instant.parse("2024-08-02T09:57:00Z"), // Start time
                    Instant.parse("2024-08-02T09:57:10Z"), // First success
                    Instant.parse("2024-08-02T09:57:20Z"), // Second success
                    Instant.parse("2024-08-02T09:57:30Z"), // Third success
                    Instant.parse("2024-08-02T09:58:40Z")) // Finish time
                    .iterator()::next;

            // When
            retryWithIdleTimeout(60, timeSupplier, messages);

            // Then
            assertThat(successfulMessages).containsExactly("m1", "m2", "m3");
            assertThat(failedMessages).isEmpty();
        }

        @Test
        void shouldTimeoutWhenMessageNotReceivedAfterIdleTimeout() throws Exception {
            // Given
            ParameterSupplier<Message> messages = messages(
                    noMessage(),
                    noMessage());
            Supplier<Instant> timeSupplier = List.of(
                    Instant.parse("2024-08-02T09:57:00Z"), // Start time
                    Instant.parse("2024-08-02T09:57:30Z"), // First check
                    Instant.parse("2024-08-02T09:58:30Z")) // Finish time
                    .iterator()::next;

            // When
            retryWithIdleTimeout(60, timeSupplier, messages);

            // Then
            assertThat(successfulMessages).isEmpty();
            assertThat(failedMessages).isEmpty();
        }

        @Test
        void shouldResetTimeoutWhenMessageReceivedInBetweenNoMessages() throws Exception {
            // Given
            ParameterSupplier<Message> messages = messages(
                    noMessage(),
                    success("m1"),
                    noMessage(),
                    noMessage());
            Supplier<Instant> timeSupplier = List.of(
                    Instant.parse("2024-08-02T09:57:00Z"), // Start time
                    Instant.parse("2024-08-02T09:57:50Z"), // First check
                    Instant.parse("2024-08-02T09:58:00Z"), // Second check
                    Instant.parse("2024-08-02T09:58:30Z"), // Third check
                    Instant.parse("2024-08-02T09:59:00Z")) // Finish time
                    .iterator()::next;

            // When
            retryWithIdleTimeout(60, timeSupplier, messages);

            // Then
            assertThat(successfulMessages).containsExactly("m1");
            assertThat(failedMessages).isEmpty();
        }
    }

    @Nested
    @DisplayName("Throttling timeout")
    class ThrottlingTimeout {
        @Test
        void shouldRetryOnceWhenThrottlingExceptionThrownThenSucceeds() throws Exception {
            // Given
            ParameterSupplier<Message> messages = messages(
                    throttlingFailureOnce("m1"),
                    success("m2"),
                    noMessage());
            Supplier<Instant> timeSupplier = List.of(
                    Instant.parse("2024-08-02T09:57:00Z"), // Start time
                    Instant.parse("2024-08-02T09:57:10Z"), // First message
                    Instant.parse("2024-08-02T09:57:20Z"), // Second message
                    Instant.parse("2024-08-02T09:58:30Z")) // Finish time
                    .iterator()::next;

            // When
            retryWithIdleTimeout(60, timeSupplier, messages);

            // Then
            assertThat(successfulMessages).containsExactly("m1", "m2");
            assertThat(failedMessages).containsExactly("m1");
        }

        @Test
        void shouldExitEarlyWhenThrottlingExceptionThrownTwiceExceedingTotalWaitTime() throws Exception {
            // Given
            ParameterSupplier<Message> messages = messages(
                    throttlingFailure("m1"),
                    throttlingFailure("m2"),
                    success("m3"),
                    noMessage());
            Supplier<Instant> timeSupplier = List.of(
                    Instant.parse("2024-08-02T09:57:00Z"), // Start time
                    Instant.parse("2024-08-02T09:59:00Z"), // First message run 1
                    Instant.parse("2024-08-02T09:59:30Z"), // First message run 2
                    Instant.parse("2024-08-02T10:02:00Z"), // Second message run 1
                    Instant.parse("2024-08-02T10:02:30Z"), // Second message run 2
                    Instant.parse("2024-08-02T10:03:00Z"), // Third message
                    Instant.parse("2024-08-02T10:10:00Z")) // Finish time
                    .iterator()::next;

            // When
            retryWithThrottlingTimeout(60, timeSupplier, messages);

            // Then
            assertThat(successfulMessages).isEmpty();
            assertThat(failedMessages).containsExactly("m1", "m1", "m2", "m2");
        }

        @Test
        void shouldExitEarlyWhenThrottlingTimeoutHappensTwiceWithSuccessfulMessageInBetween() throws Exception {
            // Given
            ParameterSupplier<Message> messages = messages(
                    throttlingFailure("m1"),
                    success("m2"),
                    throttlingFailure("m3"),
                    success("m4"),
                    noMessage());
            Supplier<Instant> timeSupplier = List.of(
                    Instant.parse("2024-08-02T09:57:00Z"), // Start time
                    Instant.parse("2024-08-02T09:59:00Z"), // First message run 1
                    Instant.parse("2024-08-02T09:59:30Z"), // First message run 2
                    Instant.parse("2024-08-02T10:02:00Z"), // Second message
                    Instant.parse("2024-08-02T10:03:00Z"), // Third message run 1
                    Instant.parse("2024-08-02T10:03:30Z"), // Third message run 2
                    Instant.parse("2024-08-02T10:10:00Z")) // Finish time
                    .iterator()::next;

            // When
            retryWithThrottlingTimeout(60, timeSupplier, messages);

            // Then
            assertThat(successfulMessages).containsExactly("m2");
            assertThat(failedMessages).containsExactly("m1", "m1", "m3", "m3");
        }
    }

    @Nested
    @DisplayName("Retry check")
    class RetryCheck {
        @Test
        void shouldNotRetryMessagesIfRetryCheckConditionMet() throws Exception {
            // Given
            ParameterSupplier<Message> messages = messages(
                    success("m1"),
                    success("m2"),
                    noMessage());
            Supplier<Boolean> retryCheck = List.of(true, false).iterator()::next;

            // When
            retryWithCheck(retryCheck, messages);

            // Then
            assertThat(successfulMessages).containsExactly("m1");
            assertThat(failedMessages).isEmpty();
        }
    }

    @Test
    void shouldExitEarlyWhenOtherExceptionThrown() throws Exception {
        // Given
        RuntimeException customException = new RuntimeException("Other exception");
        ParameterSupplier<Message> messages = messages(
                otherFailure("m1", customException),
                success("m2"),
                noMessage());
        Supplier<Instant> timeSupplier = List.of(
                Instant.parse("2024-08-02T09:57:00Z"), // Start time
                Instant.parse("2024-08-02T09:57:10Z"), // First message
                Instant.parse("2024-08-02T09:58:30Z")) // Finish time
                .iterator()::next;

        // When
        assertThatThrownBy(() -> retryWithIdleTimeout(60, timeSupplier, messages))
                .isInstanceOf(RuntimeException.class)
                .hasCause(customException);

        // Then
        assertThat(failedMessages).containsExactly("m1");
    }

    private void retryWithCheck(Supplier<Boolean> retryCheck, ParameterSupplier<Message> messages) throws Exception {
        new DynamoDBRetryWithTimeout(60000, 60000, retryCheck, () -> {
        }, Instant::now, PollWithRetries.immediateRetries(1))
                .run(messages, runner());
    }

    private void retryWithIdleTimeout(long minWaitTimeSeconds, Supplier<Instant> timeSupplier, ParameterSupplier<Message> messages) throws InterruptedException {
        new DynamoDBRetryWithTimeout(minWaitTimeSeconds, 60000, () -> true, () -> {
        }, timeSupplier, PollWithRetries.immediateRetries(1))
                .run(messages, runner());
    }

    private void retryWithThrottlingTimeout(long minWaitThrottlingSeconds, Supplier<Instant> timeSupplier, ParameterSupplier<Message> messages) throws InterruptedException {
        new DynamoDBRetryWithTimeout(60000, minWaitThrottlingSeconds, () -> true, () -> {
        }, timeSupplier, PollWithRetries.immediateRetries(1))
                .run(messages, runner());
    }

    private ParameterSupplier<Message> messages(Message... messages) {
        return Stream.of(messages).map(Optional::ofNullable).iterator()::next;
    }

    private DynamoRunner<Message> runner() {
        return message -> {
            Optional<RuntimeException> exceptionOpt = message.exceptions.get();
            if (exceptionOpt.isPresent()) {
                failedMessages.add(message.messageId);
                throw exceptionOpt.get();
            } else {
                successfulMessages.add(message.messageId);
            }
        };
    }

    private Message noMessage() {
        return null;
    }

    private Message success(String messageId) {
        return new Message(messageId, () -> Optional.empty());
    }

    private Message throttlingFailureOnce(String messageId) {
        AmazonDynamoDBException exception = new AmazonDynamoDBException("Throttling exception");
        exception.setErrorCode("ThrottlingException");
        ExceptionSupplier supplier = Stream.of((RuntimeException) exception, null)
                .map(Optional::ofNullable).iterator()::next;
        return new Message(messageId, supplier);
    }

    private Message throttlingFailure(String messageId) {
        AmazonDynamoDBException exception = new AmazonDynamoDBException("Throttling exception");
        exception.setErrorCode("ThrottlingException");
        return new Message(messageId, () -> Optional.of(exception));
    }

    private Message otherFailure(String messageId, RuntimeException e) {
        return new Message(messageId, () -> Optional.of(e));
    }

    private static class Message {
        private final String messageId;
        private final ExceptionSupplier exceptions;

        Message(String messageId, ExceptionSupplier exception) {
            this.exceptions = exception;
            this.messageId = messageId;
        }
    }

    private interface ExceptionSupplier extends Supplier<Optional<RuntimeException>> {
    }
}
