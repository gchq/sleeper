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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import sleeper.clients.util.ClientUtils;
import sleeper.compaction.job.CompactionJobSerDe;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.query.model.QuerySerDe;
import sleeper.splitter.find.SplitPartitionJobDefinitionSerDe;
import sleeper.task.common.QueueMessageCount;

import java.io.IOException;
import java.util.function.Function;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_DLQ_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_DLQ_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_JOB_DLQ_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_DLQ_URL;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

/**
 * A utility class to report information about messages on the various dead-letter
 * queues and to print out the messages in a human-readable form.
 */
public class DeadLettersStatusReport {
    private final InstanceProperties instanceProperties;
    private final AmazonSQS sqsClient;
    private final TablePropertiesProvider tablePropertiesProvider;

    public DeadLettersStatusReport(AmazonSQS sqsClient,
            InstanceProperties instanceProperties,
            TablePropertiesProvider tablePropertiesProvider) {
        this.sqsClient = sqsClient;
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
    }

    public void run() {
        System.out.println("\nDead Letters Status Report:\n--------------------------");
        printStats(instanceProperties.get(COMPACTION_JOB_DLQ_URL), "compaction jobs dead-letter", s -> {
            try {
                return CompactionJobSerDe.deserialiseFromString(s).toString();
            } catch (IOException e) {
                return e.getMessage();
            }
        });
        printStats(instanceProperties.get(INGEST_JOB_DLQ_URL), "ingest jobs dead-letter", s -> s);
        printStats(instanceProperties.get(PARTITION_SPLITTING_JOB_DLQ_URL), "partition splitting jobs dead-letter",
                s -> new SplitPartitionJobDefinitionSerDe(tablePropertiesProvider).fromJson(s).toString());

        printStats(instanceProperties.get(QUERY_DLQ_URL), "queries dead-letter", s -> new QuerySerDe(tablePropertiesProvider).fromJsonOrLeafQuery(s).toString());
    }

    private void printStats(String queueUrl, String description, Function<String, String> decoder) {
        if (queueUrl == null) {
            return;
        }
        QueueMessageCount stats = QueueMessageCount.withSqsClient(sqsClient).getQueueMessageCount(queueUrl);
        System.out.println("Messages on the " + description + " queue:" + stats);

        if (stats.getApproximateNumberOfMessages() > 0) {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                    .withQueueUrl(queueUrl)
                    .withMaxNumberOfMessages(10)
                    .withVisibilityTimeout(1);
            ReceiveMessageResult result = sqsClient.receiveMessage(receiveMessageRequest);
            for (Message message : result.getMessages()) {
                System.out.println(decoder.apply(message.getBody()));
            }
        }
    }

    public static void main(String[] args) {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        AmazonSQS sqsClient = buildAwsV1Client(AmazonSQSClientBuilder.standard());

        try {
            InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(s3Client, args[0]);
            TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient);
            DeadLettersStatusReport statusReport = new DeadLettersStatusReport(sqsClient, instanceProperties, tablePropertiesProvider);
            statusReport.run();
        } finally {
            s3Client.shutdown();
            dynamoDBClient.shutdown();
            sqsClient.shutdown();
        }
    }
}
