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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.job.common.QueueMessageCount.approximateNumberVisibleAndNotVisible;
import static sleeper.job.common.QueueMessageCountsInMemory.visibleMessages;

class QueueMessageCountsSequenceTest {

    @Test
    void shouldRetrieveMessageCountsForDifferentQueuesInSequence() {
        QueueMessageCount.Client client = QueueMessageCountsSequence.inOrder(
                visibleMessages("first-queue", 1),
                visibleMessages("second-queue", 2));

        assertThat(client.getQueueMessageCount("first-queue"))
                .isEqualTo(approximateNumberVisibleAndNotVisible(1, 0));
        assertThat(client.getQueueMessageCount("second-queue"))
                .isEqualTo(approximateNumberVisibleAndNotVisible(2, 0));
    }

    @Test
    void shouldReportQueueDoesNotExistIfCalledOutOfSequence() {
        QueueMessageCount.Client client = QueueMessageCountsSequence.inOrder(
                visibleMessages("first-queue", 1),
                visibleMessages("second-queue", 2));

        assertThatThrownBy(() -> client.getQueueMessageCount("second-queue"))
                .isInstanceOf(QueueDoesNotExistException.class)
                .hasMessageStartingWith("Queue does not exist: second-queue");
    }

    @Test
    void shouldReportQueueDoesNotExistIfCalledWithEmptySequence() {
        QueueMessageCount.Client client = QueueMessageCountsSequence.inOrder();

        assertThatThrownBy(() -> client.getQueueMessageCount("some-queue"))
                .isInstanceOf(QueueDoesNotExistException.class)
                .hasMessageStartingWith("Queue does not exist: some-queue");
    }
}
