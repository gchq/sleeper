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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import sleeper.systemtest.configuration.DataGenerationJob;
import sleeper.systemtest.configuration.SystemTestPropertyValues;

import java.util.List;
import java.util.function.Consumer;

import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_JOBS_QUEUE_URL;

public class ECSSystemTestTask {
    public static final Logger LOGGER = LoggerFactory.getLogger(ECSSystemTestTask.class);
    private final SystemTestPropertyValues properties;
    private final SqsClient sqsClient;
    private final Consumer<DataGenerationJob> jobRunner;

    public ECSSystemTestTask(SystemTestPropertyValues properties, SqsClient sqsClient, Consumer<DataGenerationJob> jobRunner) {
        this.properties = properties;
        this.sqsClient = sqsClient;
        this.jobRunner = jobRunner;
    }

    public void run() {
        ReceiveMessageResponse receiveMessageResponse = sqsClient.receiveMessage(builder -> builder
                .queueUrl(properties.get(SYSTEM_TEST_JOBS_QUEUE_URL))
                .maxNumberOfMessages(1)
                .waitTimeSeconds(20));

        List<Message> messages = receiveMessageResponse.messages();
        if (messages.isEmpty()) {
            LOGGER.info("Finishing as no jobs have been received");
            return;
        }
        Message message = messages.get(0);
        LOGGER.info("Received message {}", message.body());
    }
}
