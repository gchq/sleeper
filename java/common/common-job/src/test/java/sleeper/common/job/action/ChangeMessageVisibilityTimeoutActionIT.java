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
package sleeper.common.job.action;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import sleeper.localstack.test.LocalStackTestBase;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class ChangeMessageVisibilityTimeoutActionIT extends LocalStackTestBase {

    private String createQueueWithVisibilityTimeoutInSeconds(String timeout) {
        Map<QueueAttributeName, String> attributes = new HashMap<>();
        attributes.put(QueueAttributeName.VISIBILITY_TIMEOUT, timeout);
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(UUID.randomUUID().toString())
                .attributes(attributes)
                .build();
        return sqsClientV2.createQueue(createQueueRequest).queueUrl();
    }

    private void sendMessage(String queueUrl, String message) {
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .build();
        sqsClientV2.sendMessage(sendMessageRequest);
    }

    private ReceiveMessageResponse receiveMessage(String queueUrl) {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)
                .build();
        return sqsClientV2.receiveMessage(receiveMessageRequest);
    }

    @Test
    void shouldSeeMessageHiddenWithinCreatedTimeout() {
        // Given
        String queueUrl = createQueueWithVisibilityTimeoutInSeconds("1");
        sendMessage(queueUrl, "A message");

        // When
        receiveMessage(queueUrl);

        // Then
        assertThat(receiveMessage(queueUrl).messages()).isEmpty();
    }

    @Test
    void shouldSeeMessageReappearsAfterCreatedTimeout() throws InterruptedException {
        // Given
        String queueUrl = createQueueWithVisibilityTimeoutInSeconds("1");
        sendMessage(queueUrl, "A message");

        // When
        receiveMessage(queueUrl);
        Thread.sleep(2000);

        // Then
        assertThat(receiveMessage(queueUrl).messages()).hasSize(1);
    }

    @Test
    void shouldSeeMessageHiddenWithinUpdatedTimeout() throws ActionException, InterruptedException {
        // Given
        String queueUrl = createQueueWithVisibilityTimeoutInSeconds("1");
        sendMessage(queueUrl, "A message");

        // When
        String receiptHandle = receiveMessage(queueUrl).messages().get(0).receiptHandle();
        new MessageReference(sqsClientV2, queueUrl, "test", receiptHandle)
                .changeVisibilityTimeoutAction(10).call();
        Thread.sleep(2000);

        // Then
        assertThat(receiveMessage(queueUrl).messages()).isEmpty();
    }

    @Test
    void shouldSeeMessageReappearsAfterUpdatedTimeout() throws ActionException, InterruptedException {
        // Given
        String queueUrl = createQueueWithVisibilityTimeoutInSeconds("1");
        sendMessage(queueUrl, "A message");

        // When
        String receiptHandle = receiveMessage(queueUrl).messages().get(0).receiptHandle();
        new MessageReference(sqsClientV2, queueUrl, "test", receiptHandle)
                .changeVisibilityTimeoutAction(2).call();
        Thread.sleep(3000);

        // Then
        assertThat(receiveMessage(queueUrl).messages()).hasSize(1);
    }
}
