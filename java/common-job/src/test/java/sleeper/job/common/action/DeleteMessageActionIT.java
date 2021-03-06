/*
 * Copyright 2022 Crown Copyright
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

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import static org.junit.Assert.assertEquals;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import sleeper.core.CommonTestConstants;

public class DeleteMessageActionIT {
    
    @ClassRule
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(
            LocalStackContainer.Service.SQS
    );

    private AmazonSQS createSQSClient() {
        return AmazonSQSClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.SQS))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    private String createQueue(AmazonSQS sqs) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("VisibilityTimeout", "5");
        CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName(UUID.randomUUID().toString())
                .withAttributes(attributes);
        return sqs.createQueue(createQueueRequest).getQueueUrl();
    }
    
    @Test
    public void shouldChangeMessageVisibilityTimeout() throws InterruptedException, ActionException {
        // Given
        //  - Create queue with visibility timeout of 5 seconds
        AmazonSQS sqs = createSQSClient();
        String queueUrl = createQueue(sqs);
        //  - Put message on to queue
        String message = "A message";
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody(message);
        sqs.sendMessage(sendMessageRequest);
        //  - Retrieve message from queue
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withMaxNumberOfMessages(1);
        ReceiveMessageResult result = sqs.receiveMessage(receiveMessageRequest);
        assertEquals(1, result.getMessages().size());
        String receiptHandle = result.getMessages().get(0).getReceiptHandle();
        
        // When
        //  - Delete the message
        DeleteMessageAction action = new DeleteMessageAction(sqs, queueUrl, "test", receiptHandle);
        action.call();
        
        // Then
        // - Sleep for 6 seconds, then check that message has not reappeared
        Thread.sleep(6000L);
        receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withMaxNumberOfMessages(1)
                .withWaitTimeSeconds(0);
        result = sqs.receiveMessage(receiveMessageRequest);
        assertEquals(0, result.getMessages().size());
        
        sqs.shutdown();
    }
}
