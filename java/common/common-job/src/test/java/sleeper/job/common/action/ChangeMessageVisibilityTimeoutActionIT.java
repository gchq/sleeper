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
package sleeper.job.common.action;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.junit.jupiter.api.Test;

import sleeper.localstack.test.LocalStackTestBase;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class ChangeMessageVisibilityTimeoutActionIT extends LocalStackTestBase {

    private String createQueueWithVisibilityTimeoutInSeconds(int timeout) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("VisibilityTimeout", "" + timeout);
        CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName(UUID.randomUUID().toString())
                .withAttributes(attributes);
        return sqsClient.createQueue(createQueueRequest).getQueueUrl();
    }

    private void sendMessage(String queueUrl, String message) {
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody(message);
        sqsClient.sendMessage(sendMessageRequest);
    }

    private ReceiveMessageResult receiveMessage(String queueUrl) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withMaxNumberOfMessages(1);
        return sqsClient.receiveMessage(receiveMessageRequest);
    }

    @Test
    void shouldSeeMessageHiddenWithinCreatedTimeout() {
        // Given
        String queueUrl = createQueueWithVisibilityTimeoutInSeconds(1);
        sendMessage(queueUrl, "A message");

        // When
        receiveMessage(queueUrl);

        // Then
        assertThat(receiveMessage(queueUrl).getMessages()).isEmpty();
    }

    @Test
    void shouldSeeMessageReappearsAfterCreatedTimeout() throws InterruptedException {
        // Given
        String queueUrl = createQueueWithVisibilityTimeoutInSeconds(1);
        sendMessage(queueUrl, "A message");

        // When
        receiveMessage(queueUrl);
        Thread.sleep(2000);

        // Then
        assertThat(receiveMessage(queueUrl).getMessages()).hasSize(1);
    }

    @Test
    void shouldSeeMessageHiddenWithinUpdatedTimeout() throws ActionException, InterruptedException {
        // Given
        String queueUrl = createQueueWithVisibilityTimeoutInSeconds(1);
        sendMessage(queueUrl, "A message");

        // When
        String receiptHandle = receiveMessage(queueUrl).getMessages().get(0).getReceiptHandle();
        new MessageReference(sqsClient, queueUrl, "test", receiptHandle)
                .changeVisibilityTimeoutAction(10).call();
        Thread.sleep(2000);

        // Then
        assertThat(receiveMessage(queueUrl).getMessages()).isEmpty();
    }

    @Test
    void shouldSeeMessageReappearsAfterUpdatedTimeout() throws ActionException, InterruptedException {
        // Given
        String queueUrl = createQueueWithVisibilityTimeoutInSeconds(1);
        sendMessage(queueUrl, "A message");

        // When
        String receiptHandle = receiveMessage(queueUrl).getMessages().get(0).getReceiptHandle();
        new MessageReference(sqsClient, queueUrl, "test", receiptHandle)
                .changeVisibilityTimeoutAction(2).call();
        Thread.sleep(3000);

        // Then
        assertThat(receiveMessage(queueUrl).getMessages()).hasSize(1);
    }
}
