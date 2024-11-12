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
package sleeper.clients.status.report;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.HashSet;
import java.util.Set;

import static sleeper.clients.util.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_QUEUE_URL;

/**
 * A utility class to take messages off a dead-letter queue and send them back
 * to the original queue.
 */
public class RetryMessages {
    private final InstanceProperties instanceProperties;
    private final SqsClient sqsClient;
    private final String stack;
    private final int maxMessages;

    public RetryMessages(InstanceProperties instanceProperties, SqsClient sqsClient, String stack, int maxMessages) {
        this.instanceProperties = instanceProperties;
        this.sqsClient = sqsClient;
        this.stack = stack;
        this.maxMessages = maxMessages;
    }

    public void run() {
        Pair<String, String> queueAndDLQueueUrls = getQueueAndDLQueueUrls(stack);
        String originalQueueUrl = queueAndDLQueueUrls.getLeft();
        String deadLetterUrl = queueAndDLQueueUrls.getRight();

        int count = 0;
        while (count < maxMessages) {
            ReceiveMessageResponse response = sqsClient.receiveMessage(request -> request
                    .queueUrl(deadLetterUrl).maxNumberOfMessages(Math.min(maxMessages, 10)).waitTimeSeconds(1));
            if (response.messages().isEmpty()) {
                System.out.println("Received no messages, terminating");
                break;
            }
            System.out.println("Received " + response.messages().size() + " messages");
            for (Message message : response.messages()) {
                System.out.println("Received message with id " + message.messageId());
                sqsClient.sendMessage(request -> request.queueUrl(originalQueueUrl).messageBody(message.body()));
                System.out.println("Sent message back to original queue");
                count++;
            }
        }
    }

    private Pair<String, String> getQueueAndDLQueueUrls(String stack) {
        switch (stack) {
            case "compaction":
                return new ImmutablePair<>(instanceProperties.get(COMPACTION_JOB_QUEUE_URL), instanceProperties.get(COMPACTION_JOB_DLQ_URL));
            case "ingest":
                return new ImmutablePair<>(instanceProperties.get(INGEST_JOB_QUEUE_URL), instanceProperties.get(INGEST_JOB_DLQ_URL));
            case "query":
                return new ImmutablePair<>(instanceProperties.get(QUERY_QUEUE_URL), instanceProperties.get(QUERY_DLQ_URL));
            default:
                throw new IllegalArgumentException("Unknown stack");
        }
    }

    public static void main(String[] args) {
        if (3 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id> [compaction|ingest|query] <max-messages>");
        }
        Set<String> validStacks = new HashSet<>();
        validStacks.add("compaction");
        validStacks.add("ingest");
        validStacks.add("query");
        String stack = args[1];
        if (!validStacks.contains(stack)) {
            System.out.println("Invalid stack: must be one of compaction, ingest, query.");
            return;
        }
        int maxMessages = Integer.parseInt(args[2]);

        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        InstanceProperties instanceProperties;
        try {
            instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, args[0]);
        } finally {
            s3Client.shutdown();
        }

        try (SqsClient sqsClient = buildAwsV2Client(SqsClient.builder())) {
            RetryMessages retryMessages = new RetryMessages(instanceProperties, sqsClient, stack, maxMessages);
            retryMessages.run();
        }
    }
}
