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
import sleeper.core.util.PollWithRetries.TimedOutException;

import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DynamoDBUtilsTest {

    @Nested
    @DisplayName("Find throttling exception")
    class FindThrottlingException {

        @Test
        void shouldFindThrottlingExceptionWithNoCauses() {
            // Given
            AmazonDynamoDBException exception = new AmazonDynamoDBException("Throttling exception");
            exception.setErrorCode("ThrottlingException");

            // When / Then
            assertThat(DynamoDBUtils.isThrottlingException(exception)).isTrue();
        }

        @Test
        void shouldFindThrottlingExceptionWhenItIsRootCause() {
            // Given
            AmazonDynamoDBException rootCause = new AmazonDynamoDBException("Throttling exception");
            rootCause.setErrorCode("ThrottlingException");
            Exception cause = new Exception("First cause exception", rootCause);
            Exception exception = new Exception("Test exception", cause);

            // When / Then
            assertThat(DynamoDBUtils.isThrottlingException(exception)).isTrue();
        }

        @Test
        void shouldNotFindThrottlingExceptionWithMultipleCauses() {
            // Given
            Exception rootCause = new Exception("Root cause exception");
            Exception cause = new Exception("First cause exception", rootCause);
            Exception exception = new Exception("Test exception", cause);

            // When / Then
            assertThat(DynamoDBUtils.isThrottlingException(exception)).isFalse();
        }

        @Test
        void shouldNotFindThrottlingExceptionWithNoCause() {
            // Given
            Exception exception = new Exception("Test exception");

            // When / Then
            assertThat(DynamoDBUtils.isThrottlingException(exception)).isFalse();
        }

        @Test
        void shouldNotFindThrottlingExceptionWhenExceptionHasDifferentErrorCode() {
            // Given
            AmazonDynamoDBException exception = new AmazonDynamoDBException("Conditional check exception");
            exception.setErrorCode("ConditionalCheckFailedException");

            // When / Then
            assertThat(DynamoDBUtils.isThrottlingException(exception)).isFalse();
        }
    }

    @Nested
    @DisplayName("Retry on throttling exception")
    class RetryOnThrottlingException {
        @Test
        void shouldNotTimeoutWhenNoExceptionsThrown() {
            // Given
            Runnable runnable = () -> {
            };

            // When / Then
            assertThatCode(() -> retryOnceOnThrottlingException(runnable))
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldTimeoutWhenThrottlingExceptionThrownTooManyTimes() {
            // Given
            Runnable runnable = () -> {
                AmazonDynamoDBException exception = new AmazonDynamoDBException("Throttling exception");
                exception.setErrorCode("ThrottlingException");
                throw exception;
            };

            // When / Then
            assertThatThrownBy(() -> retryOnceOnThrottlingException(runnable))
                    .isInstanceOf(TimedOutException.class);
        }

        @Test
        void shouldThrowWrappedExceptionWhenOtherExceptionThrown() {
            // Given
            RuntimeException exception = new RuntimeException("Custom runtime exception");
            Runnable runnable = () -> {
                throw exception;
            };

            // When / Then
            assertThatThrownBy(() -> retryOnceOnThrottlingException(runnable))
                    .isInstanceOf(RuntimeException.class)
                    .hasCause(exception);
        }

        @Test
        void shouldNotTimeoutWhenThrottlingExceptionThrownThenNoExceptionThrown() {
            // Given
            AmazonDynamoDBException throttlingException = new AmazonDynamoDBException("Throttling exception");
            throttlingException.setErrorCode("ThrottlingException");
            Iterator<RuntimeException> throwables = List.of((RuntimeException) throttlingException).iterator();
            Runnable runnable = () -> {
                if (throwables.hasNext()) {
                    throw throwables.next();
                }
            };

            // When / Then
            assertThatCode(() -> retryOnceOnThrottlingException(runnable))
                    .doesNotThrowAnyException();
        }

        private void retryOnceOnThrottlingException(Runnable runnable) throws InterruptedException {
            DynamoDBUtils.retryOnThrottlingException(PollWithRetries.immediateRetries(1), runnable);
        }
    }
}
