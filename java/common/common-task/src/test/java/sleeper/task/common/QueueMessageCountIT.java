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

package sleeper.task.common;

import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import org.junit.jupiter.api.Test;

import sleeper.localstack.test.LocalStackTestBase;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueueMessageCountIT extends LocalStackTestBase {

    private String createQueue() {
        return sqsClient.createQueue(UUID.randomUUID().toString()).getQueueUrl();
    }

    @Test
    void shouldReportNoMessagesWhenQueueIsEmpty() {
        // Given
        String queueUrl = createQueue();

        // When
        int numberOfMessages = QueueMessageCount.withSqsClient(sqsClient).getQueueMessageCount(queueUrl)
                .getApproximateNumberOfMessages();

        // Then
        assertThat(numberOfMessages).isZero();
    }

    @Test
    void shouldReportNumberOfMessagesWhenQueueIsNotEmpty() {
        // Given
        String queueUrl = createQueue();
        for (int i = 1; i <= 10; i++) {
            sqsClient.sendMessage(queueUrl, "{testMessageId:" + i + "}");
        }

        // When
        int numberOfMessages = QueueMessageCount.withSqsClient(sqsClient).getQueueMessageCount(queueUrl)
                .getApproximateNumberOfMessages();

        // Then
        assertThat(numberOfMessages).isEqualTo(10);
    }

    @Test
    void shouldReportNumberOfMessagesWhenSomeMessagesHaveBeenProcessed() {
        // Given
        String queueUrl = createQueue();
        for (int i = 1; i <= 10; i++) {
            sqsClient.sendMessage(queueUrl, "{testMessageId:" + i + "}");
        }
        for (int i = 1; i <= 3; i++) {
            sqsClient.receiveMessage(queueUrl);
        }

        // When
        int numberOfMessages = QueueMessageCount.withSqsClient(sqsClient).getQueueMessageCount(queueUrl)
                .getApproximateNumberOfMessages();

        // Then
        assertThat(numberOfMessages).isEqualTo(7);
    }

    @Test
    void shouldFailWhenQueueDoesNotExist() {
        // Given
        QueueMessageCount.Client queueClient = QueueMessageCount.withSqsClient(sqsClient);

        // When / Then
        assertThatThrownBy(() -> queueClient.getQueueMessageCount("non-existent-queue"))
                .isInstanceOf(QueueDoesNotExistException.class);
    }
}
