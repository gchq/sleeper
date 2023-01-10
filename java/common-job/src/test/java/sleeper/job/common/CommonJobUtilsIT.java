/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.job.common;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import sleeper.core.CommonTestConstants;

import static org.assertj.core.api.Assertions.assertThat;

public class CommonJobUtilsIT {
    private static final String TEST_QUEUE_NAME = "test-queue-url";

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
        return sqs.createQueue(TEST_QUEUE_NAME).getQueueUrl();
    }

    @Test
    public void shouldReportNoMessagesWhenQueueIsEmpty() {
        // Given / When
        AmazonSQS sqsClient = createSQSClient();
        String queueUrl = createQueue(sqsClient);

        // Then
        try {
            int numberOfMessages = CommonJobUtils.getNumberOfMessagesInQueue(queueUrl, sqsClient)
                    .get(QueueAttributeName.ApproximateNumberOfMessages.toString());
            assertThat(numberOfMessages).isEqualTo(0);
        } finally {
            sqsClient.deleteQueue(queueUrl);
            sqsClient.shutdown();
        }
    }

    @Test
    public void shouldReportNumberOfMessagesWhenQueueIsNotEmpty() {
        // Given
        AmazonSQS sqsClient = createSQSClient();
        String queueUrl = createQueue(sqsClient);

        // When
        for (int i = 1; i <= 10; i++) {
            sqsClient.sendMessage(queueUrl, "{testMessageId:" + i + "}");
        }

        // Then
        try {
            int numberOfMessages = CommonJobUtils.getNumberOfMessagesInQueue(queueUrl, sqsClient)
                    .get(QueueAttributeName.ApproximateNumberOfMessages.toString());
            assertThat(numberOfMessages).isEqualTo(10);
        } finally {
            sqsClient.deleteQueue(queueUrl);
            sqsClient.shutdown();
        }
    }

    @Test
    public void shouldReportNumberOfMessagesWhenSomeMessagesHasBeenProcessed() {
        // Given
        AmazonSQS sqsClient = createSQSClient();
        String queueUrl = createQueue(sqsClient);

        // When
        for (int i = 1; i <= 10; i++) {
            sqsClient.sendMessage(queueUrl, "{testMessageId:" + i + "}");
        }
        for (int i = 1; i <= 3; i++) {
            sqsClient.receiveMessage(queueUrl);
        }

        // Then
        try {
            int numberOfMessages = CommonJobUtils.getNumberOfMessagesInQueue(queueUrl, sqsClient)
                    .get(QueueAttributeName.ApproximateNumberOfMessages.toString());
            assertThat(numberOfMessages).isEqualTo(7);
        } finally {
            sqsClient.deleteQueue(queueUrl);
            sqsClient.shutdown();
        }
    }
}
