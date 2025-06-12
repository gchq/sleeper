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

package sleeper.common.task;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

import sleeper.localstack.test.LocalStackTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueueMessageCountIT extends LocalStackTestBase {

    @Test
    void shouldReportNoMessagesWhenQueueIsEmpty() {
        // Given
        String queueUrl = createSqsQueueGetUrl();

        // When
        int numberOfMessages = QueueMessageCount.withSqsClient(sqsClientV2).getQueueMessageCount(queueUrl)
                .getApproximateNumberOfMessages();

        // Then
        assertThat(numberOfMessages).isZero();
    }

    @Test
    void shouldReportNumberOfMessagesWhenQueueIsNotEmpty() {
        // Given
        String queueUrl = createSqsQueueGetUrl();
        for (int i = 1; i <= 10; i++) {
            sqsClient.sendMessage(queueUrl, "{testMessageId:" + i + "}");
        }

        // When
        int numberOfMessages = QueueMessageCount.withSqsClient(sqsClientV2).getQueueMessageCount(queueUrl)
                .getApproximateNumberOfMessages();

        // Then
        assertThat(numberOfMessages).isEqualTo(10);
    }

    @Test
    void shouldReportNumberOfMessagesWhenSomeMessagesHaveBeenProcessed() {
        // Given
        String queueUrl = createSqsQueueGetUrl();
        for (int i = 1; i <= 10; i++) {
            sqsClient.sendMessage(queueUrl, "{testMessageId:" + i + "}");
        }
        for (int i = 1; i <= 3; i++) {
            sqsClient.receiveMessage(queueUrl);
        }

        // When
        int numberOfMessages = QueueMessageCount.withSqsClient(sqsClientV2).getQueueMessageCount(queueUrl)
                .getApproximateNumberOfMessages();

        // Then
        assertThat(numberOfMessages).isEqualTo(7);
    }

    @Test
    void shouldFailWhenQueueDoesNotExist() {
        // Given
        QueueMessageCount.Client queueClient = QueueMessageCount.withSqsClient(sqsClientV2);

        // When / Then
        assertThatThrownBy(() -> queueClient.getQueueMessageCount("non-existent-queue"))
                .isInstanceOf(QueueDoesNotExistException.class);
    }
}
