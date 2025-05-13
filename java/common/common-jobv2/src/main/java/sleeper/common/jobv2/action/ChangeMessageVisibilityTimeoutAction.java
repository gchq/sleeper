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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Sets the visibility timeout of an SQS message with the provided receipt handle.
 */
public class ChangeMessageVisibilityTimeoutAction implements Action {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeMessageVisibilityTimeoutAction.class);
    private static final long[] MILLISECONDS_TO_SLEEP = new long[]{2000L, 8000L, 16000L, 64000L};

    private final SqsClient sqsClient;
    private final String sqsJobQueueUrl;
    private final String description;
    private final String messageReceiptHandle;
    private final int messageVisibilityTimeout;

    public ChangeMessageVisibilityTimeoutAction(MessageReference message, int messageVisibilityTimeout) {
        sqsClient = message.getSqsClient();
        sqsJobQueueUrl = message.getSqsJobQueueUrl();
        description = message.getJobDescription();
        messageReceiptHandle = message.getReceiptHandle();
        this.messageVisibilityTimeout = messageVisibilityTimeout;
    }

    @Override
    public void call() throws ActionException {
        ChangeMessageVisibilityRequest changeMessageVisibilityRequest = ChangeMessageVisibilityRequest.builder()
                .queueUrl(sqsJobQueueUrl)
                .visibilityTimeout(messageVisibilityTimeout)
                .receiptHandle(messageReceiptHandle)
                .build();
        int count = 0;
        SqsException exception = null;
        while (count < 4) {
            try {
                sqsClient.changeMessageVisibility(changeMessageVisibilityRequest);
                LOGGER.info("{}: Changed message visibility timeout to {} for message with receipt handle {}",
                        description, messageVisibilityTimeout, messageReceiptHandle);
                return;
            } catch (SqsException e) {
                count++;
                exception = e;
                String stackTrace = Arrays
                        .stream(exception.getStackTrace())
                        .map(StackTraceElement::toString)
                        .collect(Collectors.joining("\n"));
                LOGGER.info("{}: AmazonSQSException changing message visibility timeout to {} for message with receipt handle {} (Exception message {}, stacktrace {})",
                        description, messageVisibilityTimeout, messageReceiptHandle, exception.getMessage(), stackTrace);
                try {
                    Thread.sleep(MILLISECONDS_TO_SLEEP[count - 1]);
                } catch (InterruptedException interruptedException) {
                    // Do nothing
                }
            }
        }
        throw new ActionException(description + ": AmazonSQSException changing message visibility timeout", exception);
    }
}
