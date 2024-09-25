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
package sleeper.clients;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.s3properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.record.Record;
import sleeper.core.record.ResultsBatch;
import sleeper.core.record.serialiser.JSONResultsBatchSerialiser;
import sleeper.core.schema.Schema;

import java.io.IOException;
import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_QUEUE_URL;

/**
 * Polls an SQS queue of query results, printing them out to the screen as they arrive.
 */
public class QueryResultsSQSQueuePoller {
    private final AmazonSQS sqsClient;
    private final String resultsSQSQueueUrl;

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryResultsSQSQueuePoller.class);

    public QueryResultsSQSQueuePoller(AmazonSQS sqsClient, String resultsSQSQueueUrl) {
        this.sqsClient = sqsClient;
        this.resultsSQSQueueUrl = resultsSQSQueueUrl;
    }

    public void run() {
        int numConsecutiveNoMessages = 0;
        while (numConsecutiveNoMessages < 15) {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                    .withQueueUrl(resultsSQSQueueUrl)
                    .withMaxNumberOfMessages(10)
                    .withWaitTimeSeconds(20);
            ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);
            if (receiveMessageResult.getMessages().isEmpty()) {
                numConsecutiveNoMessages++;
            } else {
                numConsecutiveNoMessages = 0;
            }
            LOGGER.info("{} messages received", receiveMessageResult.getMessages().size());
            for (Message message : receiveMessageResult.getMessages()) {
                String messageHandle = message.getReceiptHandle();
                String serialisedResults = message.getBody();
                JSONResultsBatchSerialiser serialiser = new JSONResultsBatchSerialiser();
                ResultsBatch resultsBatch = serialiser.deserialise(serialisedResults);
                String queryId = resultsBatch.getQueryId();
                Schema schema = resultsBatch.getSchema();
                List<Record> records = resultsBatch.getRecords();
                LOGGER.info("{} results for query {}", receiveMessageResult.getMessages().size(), queryId);
                records.forEach(r -> System.out.println(r.toString(schema)));
                sqsClient.deleteMessage(resultsSQSQueueUrl, messageHandle);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties;
        try {
            instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, args[0]);
        } finally {
            s3Client.shutdown();
        }

        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        try {
            QueryResultsSQSQueuePoller poller = new QueryResultsSQSQueuePoller(sqsClient, instanceProperties.get(QUERY_RESULTS_QUEUE_URL));
            poller.run();
        } finally {
            sqsClient.shutdown();
        }
    }
}
