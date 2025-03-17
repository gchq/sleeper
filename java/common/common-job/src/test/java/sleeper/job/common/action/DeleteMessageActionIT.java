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

public class DeleteMessageActionIT extends LocalStackTestBase {

    private String createQueue() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("VisibilityTimeout", "5");
        CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName(UUID.randomUUID().toString())
                .withAttributes(attributes);
        return sqsClient.createQueue(createQueueRequest).getQueueUrl();
    }

    @Test
    public void shouldChangeMessageVisibilityTimeout() throws InterruptedException, ActionException {
        // Given
        //  - Create queue with visibility timeout of 5 seconds
        String queueUrl = createQueue();
        //  - Put message on to queue
        String message = "A message";
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody(message);
        sqsClient.sendMessage(sendMessageRequest);
        //  - Retrieve message from queue
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withMaxNumberOfMessages(1);
        ReceiveMessageResult result = sqsClient.receiveMessage(receiveMessageRequest);
        assertThat(result.getMessages()).hasSize(1);
        String receiptHandle = result.getMessages().get(0).getReceiptHandle();

        // When
        //  - Delete the message
        DeleteMessageAction action = new MessageReference(sqsClient, queueUrl, "test", receiptHandle).deleteAction();
        action.call();

        // Then
        // - Sleep for 6 seconds, then check that message has not reappeared
        Thread.sleep(6000L);
        receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withMaxNumberOfMessages(1)
                .withWaitTimeSeconds(0);
        result = sqsClient.receiveMessage(receiveMessageRequest);
        assertThat(result.getMessages()).isEmpty();
    }
}
