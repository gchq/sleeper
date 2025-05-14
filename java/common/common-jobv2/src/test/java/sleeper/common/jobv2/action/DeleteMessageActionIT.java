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
package sleeper.common.jobv2.action;

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

public class DeleteMessageActionIT extends LocalStackTestBase {

    private String createQueue() {
        Map<QueueAttributeName, String> attributes = new HashMap<>();
        attributes.put(QueueAttributeName.VISIBILITY_TIMEOUT, "5");
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(UUID.randomUUID().toString())
                .attributes(attributes)
                .build();
        return sqsClientV2.createQueue(createQueueRequest).queueUrl();
    }

    @Test
    public void shouldChangeMessageVisibilityTimeout() throws InterruptedException, ActionException {
        // Given
        //  - Create queue with visibility timeout of 5 seconds
        String queueUrl = createQueue();
        //  - Put message on to queue
        String message = "A message";
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .build();
        sqsClientV2.sendMessage(sendMessageRequest);
        //  - Retrieve message from queue
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)
                .build();
        ReceiveMessageResponse response = sqsClientV2.receiveMessage(receiveMessageRequest);
        assertThat(response.messages()).hasSize(1);
        String receiptHandle = response.messages().get(0).receiptHandle();

        // When
        //  - Delete the message
        DeleteMessageAction action = new MessageReference(sqsClientV2, queueUrl, "test", receiptHandle).deleteAction();
        action.call();

        // Then
        // - Sleep for 6 seconds, then check that message has not reappeared
        Thread.sleep(6000L);
        receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)
                .waitTimeSeconds(0)
                .build();
        response = sqsClientV2.receiveMessage(receiveMessageRequest);
        assertThat(response.messages()).isEmpty();
    }
}
