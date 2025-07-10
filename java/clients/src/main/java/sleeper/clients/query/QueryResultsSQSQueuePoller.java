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
package sleeper.clients.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.row.ResultsBatch;
import sleeper.core.row.serialiser.JSONResultsBatchSerialiser;

import java.io.IOException;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_QUEUE_URL;

/**
 * Polls an SQS queue of query results, printing them out to the screen as they arrive.
 */
public class QueryResultsSQSQueuePoller {
    private final SqsClient sqsClient;
    private final String resultsSQSQueueUrl;

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryResultsSQSQueuePoller.class);

    public QueryResultsSQSQueuePoller(SqsClient sqsClient, String resultsSQSQueueUrl) {
        this.sqsClient = sqsClient;
        this.resultsSQSQueueUrl = resultsSQSQueueUrl;
    }

    public void run() {
        int numConsecutiveNoMessages = 0;
        while (numConsecutiveNoMessages < 15) {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(resultsSQSQueueUrl)
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(20)
                    .build();
            ReceiveMessageResponse receiveMessageResponse = sqsClient.receiveMessage(receiveMessageRequest);
            if (receiveMessageResponse.messages().isEmpty()) {
                numConsecutiveNoMessages++;
            } else {
                numConsecutiveNoMessages = 0;
            }
            LOGGER.info("{} messages received", receiveMessageResponse.messages().size());
            for (Message message : receiveMessageResponse.messages()) {
                JSONResultsBatchSerialiser serialiser = new JSONResultsBatchSerialiser();
                ResultsBatch resultsBatch = serialiser.deserialise(message.body());
                LOGGER.info("{} results for query {}", receiveMessageResponse.messages().size(), resultsBatch.getQueryId());
                resultsBatch.getRows().forEach(
                        record -> System.out.println(record.toString(resultsBatch.getSchema())));
                sqsClient.deleteMessage(request -> request
                        .queueUrl(resultsSQSQueueUrl)
                        .receiptHandle(message.receiptHandle()));
            }
        }
    }

    public static void main(String[] args) throws IOException {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }

        InstanceProperties instanceProperties;
        try (S3Client s3Client = S3Client.create()) {
            instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, args[0]);
        }

        try (SqsClient sqsClient = SqsClient.create()) {
            QueryResultsSQSQueuePoller poller = new QueryResultsSQSQueuePoller(sqsClient, instanceProperties.get(QUERY_RESULTS_QUEUE_URL));
            poller.run();
        }
    }
}
