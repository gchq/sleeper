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

package sleeper.systemtest.dsl.extension;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperty;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;

public class AfterTestPurgeQueuesTest {
    private final List<List<InstanceProperty>> queuePurges = new ArrayList<>();

    @Nested
    @DisplayName("Only purge queues when test failed")
    class OnlyPurgeQueueWhenTestFailed {
        @Test
        void shouldPurgeQueuesWhenTestFailed() {
            // When
            purgingQueues(INGEST_JOB_QUEUE_URL).testFailed();

            // Then
            assertThat(queuePurges).containsExactly(List.of(INGEST_JOB_QUEUE_URL));
        }

        @Test
        void shouldPurgeMultipleQueuesWhenTestFailed() {
            // When
            purgingQueues(INGEST_JOB_QUEUE_URL, BULK_IMPORT_EMR_JOB_QUEUE_URL).testFailed();

            // Then
            assertThat(queuePurges).containsExactly(List.of(INGEST_JOB_QUEUE_URL, BULK_IMPORT_EMR_JOB_QUEUE_URL));
        }

        @Test
        void shouldNotPurgeQueuesWhenTestPassed() {
            // When
            purgingQueues(INGEST_JOB_QUEUE_URL).testPassed();

            // Then
            assertThat(queuePurges).isEmpty();
        }

        @Test
        void shouldNotPurgeQueuesWhenNoQueuesAreSpecified() {
            // When
            purgingNoQueues().testFailed();

            // Then
            assertThat(queuePurges).isEmpty();
        }
    }

    private AfterTestPurgeQueues purgingNoQueues() {
        return new AfterTestPurgeQueues(() -> queuePurges::add);
    }

    private AfterTestPurgeQueues purgingQueues(InstanceProperty... queueProperties) {
        AfterTestPurgeQueues afterTest = purgingNoQueues();
        afterTest.purgeIfTestFailed(queueProperties);
        return afterTest;
    }
}
