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

import org.junit.jupiter.api.Test;

import sleeper.localstack.test.LocalStackTestBase;
import sleeper.systemtest.drivers.util.sqs.AwsDrainSqsQueue.EmptyQueueResults;

import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class AwsDrainSqsQueueIT extends LocalStackTestBase {

    @Test
    void shouldEmptyMultipleQueuesAcrossThreads() {
        // Given
        List<String> queueUrls = IntStream.range(0, 5)
                .mapToObj(i -> createSqsQueueGetUrl()).toList();
        sqsClient.sendMessage(queueUrls.get(0), "test0");
        sqsClient.sendMessage(queueUrls.get(1), "test1");

        // When
        EmptyQueueResults results = drainQueueWithThreads(3).empty(queueUrls);

        // Then
        List<Long> resultsInOrder = queueUrls.stream()
                .map(results::getMessagesDeleted)
                .toList();
        assertThat(resultsInOrder).containsExactly(1L, 1L, 0L, 0L, 0L);
    }

    @Test
    void shouldEmptyMultipleQueuesAcrossMoreThreadsThanQueues() {
        // Given
        List<String> queueUrls = List.of(createSqsQueueGetUrl(), createSqsQueueGetUrl());
        sqsClient.sendMessage(queueUrls.get(0), "test0");
        sqsClient.sendMessage(queueUrls.get(1), "test1");

        // When
        EmptyQueueResults results = drainQueueWithThreads(3).empty(queueUrls);

        // Then
        List<Long> resultsInOrder = queueUrls.stream()
                .map(results::getMessagesDeleted)
                .toList();
        assertThat(resultsInOrder).containsExactly(1L, 1L);
    }

    private AwsDrainSqsQueue drainQueueWithThreads(int numThreads) {
        return AwsDrainSqsQueue.builder()
                .sqsClient(sqsClientV2)
                .numThreads(numThreads)
                .messagesPerBatchPerThread(10)
                .waitTimeSeconds(1)
                .build();
    }

}
