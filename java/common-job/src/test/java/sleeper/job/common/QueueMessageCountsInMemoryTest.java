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
package sleeper.job.common;

import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.job.common.QueueMessageCount.approximateNumberVisibleAndNotVisible;

class QueueMessageCountsInMemoryTest {

    @Test
    void shouldRetrieveMessageCountsForSpecifiedQueue() {
        // Given
        QueueMessageCount.Client client = QueueMessageCountsInMemory.visibleAndNotVisibleCounts(
                "test-queue", 12, 34);

        // When / Then
        assertThat(client.getQueueMessageCount("test-queue"))
                .isEqualTo(approximateNumberVisibleAndNotVisible(12, 34));
    }

    @Test
    void shouldRetrieveVisibleMessageCountForSpecifiedQueue() {
        // Given
        QueueMessageCount.Client client = QueueMessageCountsInMemory.visibleMessages("test-queue", 42);

        // When / Then
        assertThat(client.getQueueMessageCount("test-queue"))
                .isEqualTo(approximateNumberVisibleAndNotVisible(42, 0));
    }

    @Test
    void shouldRetrieveMessageCountsForSecondSpecifiedQueue() {
        // Given
        QueueMessageCount.Client client = QueueMessageCountsInMemory.from(Map.of(
                "test-queue-1", approximateNumberVisibleAndNotVisible(12, 34),
                "test-queue-2", approximateNumberVisibleAndNotVisible(56, 78)));

        // When / Then
        assertThat(client.getQueueMessageCount("test-queue-2"))
                .isEqualTo(approximateNumberVisibleAndNotVisible(56, 78));
    }

    @Test
    void shouldFailWhenMessageCountsNotSpecifiedForQueue() {
        // Given
        QueueMessageCount.Client client = QueueMessageCountsInMemory.noQueues();

        // When / Then
        assertThatThrownBy(() -> client.getQueueMessageCount("test-queue"))
                .isInstanceOf(QueueDoesNotExistException.class);
    }
}
