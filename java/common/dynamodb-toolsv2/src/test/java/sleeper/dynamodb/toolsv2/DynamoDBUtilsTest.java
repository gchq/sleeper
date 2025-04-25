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
package sleeper.dynamodb.toolsv2;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

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
            // When / Then
            assertThat(DynamoDBUtils.isThrottlingException(buildDynamoDbException("ThrottlingException"))).isTrue();
        }

        @Test
        void shouldFindThrottlingExceptionWhenItIsRootCause() {
            // Given
            AwsServiceException rootCause = buildDynamoDbException("ThrottlingException");
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
            // When / Then
            assertThat(DynamoDBUtils.isThrottlingException(buildDynamoDbException("Conditional check exception"))).isFalse();
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
                throw buildDynamoDbException("ThrottlingException");
            };

            // When / Then
            assertThatThrownBy(() -> retryOnceOnThrottlingException(runnable))
                    .isInstanceOf(TimedOutException.class);
        }

        @Test
        void shouldPropagateThrownException() {
            // Given
            RuntimeException exception = new RuntimeException("Custom runtime exception");
            Runnable runnable = () -> {
                throw exception;
            };

            // When / Then
            assertThatThrownBy(() -> retryOnceOnThrottlingException(runnable))
                    .isSameAs(exception);
        }

        @Test
        void shouldNotTimeoutWhenThrottlingExceptionThrownThenNoExceptionThrown() {
            // Given
            AwsServiceException throttlingException = buildDynamoDbException("ThrottlingException");
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

    protected AwsServiceException buildDynamoDbException(String errorCode) {
        return DynamoDbException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(errorCode)
                        .build())
                .build();
    }
}
