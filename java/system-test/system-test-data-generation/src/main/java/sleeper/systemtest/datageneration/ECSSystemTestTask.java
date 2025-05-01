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
package sleeper.systemtest.datageneration;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.job.common.action.ActionException;
import sleeper.job.common.action.MessageReference;
import sleeper.job.common.action.thread.PeriodicActionRunnable;
import sleeper.systemtest.configuration.SystemTestDataGenerationJob;
import sleeper.systemtest.configuration.SystemTestDataGenerationJobSerDe;
import sleeper.systemtest.configuration.SystemTestPropertyValues;

import java.util.List;
import java.util.function.Consumer;

import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_JOBS_QUEUE_URL;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_KEEP_ALIVE_PERIOD_IN_SECONDS;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;

public class ECSSystemTestTask {
    public static final Logger LOGGER = LoggerFactory.getLogger(ECSSystemTestTask.class);
    private final SystemTestPropertyValues properties;
    private final AmazonSQS sqsClient;
    private final Consumer<SystemTestDataGenerationJob> jobRunner;

    public ECSSystemTestTask(SystemTestPropertyValues properties, AmazonSQS sqsClient, Consumer<SystemTestDataGenerationJob> jobRunner) {
        this.properties = properties;
        this.sqsClient = sqsClient;
        this.jobRunner = jobRunner;
    }

    public void run() {
        String queueUrl = properties.get(SYSTEM_TEST_JOBS_QUEUE_URL);
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl)
                .withMaxNumberOfMessages(1)
                .withWaitTimeSeconds(20);
        ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);
        List<Message> messages = receiveMessageResult.getMessages();
        if (messages.isEmpty()) {
            LOGGER.info("Finishing as no jobs have been received");
            return;
        }
        Message message = messages.get(0);
        LOGGER.info("Received message {}", message.getBody());
        SystemTestDataGenerationJob job = new SystemTestDataGenerationJobSerDe().fromJson(message.getBody());

        MessageReference messageReference = new MessageReference(sqsClient, queueUrl,
                "Data generation job " + job.getJobId(), message.getReceiptHandle());
        // Create background thread to keep messages alive
        PeriodicActionRunnable keepAliveRunnable = new PeriodicActionRunnable(
                messageReference.changeVisibilityTimeoutAction(properties.getInt(SYSTEM_TEST_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)),
                properties.getInt(SYSTEM_TEST_KEEP_ALIVE_PERIOD_IN_SECONDS));
        keepAliveRunnable.start();
        try {
            jobRunner.accept(job);
            messageReference.deleteAction().call();
        } catch (ActionException e) {
            throw new RuntimeException(e);
        } finally {
            keepAliveRunnable.stop();
        }
    }
}
