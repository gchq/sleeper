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

import sleeper.configuration.properties.instance.InstanceProperty;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;

public class PurgeQueueOnTestFailureExtensionTest {
    private final Map<InstanceProperty, Integer> messageCountsByQueueProperty = new HashMap<>();

    @Nested
    @DisplayName("Only purge queues when test failed")
    class OnlyPurgeQueueWhenTestFailed {
        @Test
        void shouldPurgeQueuesWhenTestFailed() throws Exception {
            // Given
            PurgeQueueOnTestFailureExtension purgeQueueExtension = createExtensionPurgingQueue(INGEST_JOB_QUEUE_URL);
            messageCountsByQueueProperty.put(INGEST_JOB_QUEUE_URL, 123);
            messageCountsByQueueProperty.put(BULK_IMPORT_EMR_JOB_QUEUE_URL, 456);

            // When
            purgeQueueExtension.afterTestFailed();

            // Then
            assertThat(messageCountsByQueueProperty).isEqualTo(
                    Map.of(BULK_IMPORT_EMR_JOB_QUEUE_URL, 456));
        }

        @Test
        void shouldNotPurgeQueuesWhenTestPassed() {
            // Given
            PurgeQueueOnTestFailureExtension purgeQueueExtension = createExtensionPurgingQueue(INGEST_JOB_QUEUE_URL);
            messageCountsByQueueProperty.put(INGEST_JOB_QUEUE_URL, 123);
            messageCountsByQueueProperty.put(BULK_IMPORT_EMR_JOB_QUEUE_URL, 456);

            // When
            purgeQueueExtension.afterTestPassed();

            // Then
            assertThat(messageCountsByQueueProperty).isEqualTo(Map.of(
                    INGEST_JOB_QUEUE_URL, 123,
                    BULK_IMPORT_EMR_JOB_QUEUE_URL, 456));
        }
    }

    private PurgeQueueOnTestFailureExtension createExtensionPurgingQueue(InstanceProperty queueProperty) {
        return new PurgeQueueOnTestFailureExtension(queueProperty, messageCountsByQueueProperty::remove);
    }
}
