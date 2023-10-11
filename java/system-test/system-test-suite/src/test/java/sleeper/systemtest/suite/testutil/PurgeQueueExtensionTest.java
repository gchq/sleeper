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

package sleeper.systemtest.suite.testutil;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class PurgeQueueExtensionTest {
    private static final String QUEUE_URL_1 = "test-queue-url-1";
    private static final String QUEUE_URL_2 = "test-queue-url-2";

    private final Map<String, Integer> messageCountsByQueueUrl = new HashMap<>();

    @Nested
    @DisplayName("Only purge queues when test failed")
    class OnlyPurgeQueueWhenTestFailed {
        @Test
        void shouldPurgeQueuesWhenTestFailed() {
            // Given
            PurgeQueueExtension purgeQueueExtension = createExtensionPurgingQueue(QUEUE_URL_1);
            messageCountsByQueueUrl.put(QUEUE_URL_1, 123);
            messageCountsByQueueUrl.put(QUEUE_URL_2, 456);

            // When
            purgeQueueExtension.afterTestFailed();

            // Then
            assertThat(messageCountsByQueueUrl).isEqualTo(
                    Map.of(QUEUE_URL_2, 456));
        }

        @Test
        void shouldNotPurgeQueuesWhenTestPassed() {
            // Given
            PurgeQueueExtension purgeQueueExtension = createExtensionPurgingQueue(QUEUE_URL_1);
            messageCountsByQueueUrl.put(QUEUE_URL_1, 123);
            messageCountsByQueueUrl.put(QUEUE_URL_2, 456);

            // When
            purgeQueueExtension.afterTestPassed();

            // Then
            assertThat(messageCountsByQueueUrl).isEqualTo(
                    Map.of(QUEUE_URL_1, 123, QUEUE_URL_2, 456));
        }
    }

    private PurgeQueueExtension createExtensionPurgingQueue(String queueUrl) {
        return new PurgeQueueExtension(List.of(queueUrl), messageCountsByQueueUrl::remove, () -> {
        });
    }
}
