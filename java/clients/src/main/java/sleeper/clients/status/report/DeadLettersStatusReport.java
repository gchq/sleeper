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
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.query.core.model.QuerySerDe;
import sleeper.splitter.core.find.SplitPartitionJobDefinitionSerDe;
import sleeper.task.common.QueueMessageCount;

import java.util.function.Function;

import static sleeper.clients.util.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_JOB_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_DLQ_URL;

/**
 * A utility class to report information about messages on the various dead-letter
 * queues and to print out the messages in a human-readable form.
 */
public class DeadLettersStatusReport {
    private final InstanceProperties instanceProperties;
    private final SqsClient sqsClient;
    private final QueueMessageCount.Client messageCount;
    private final TablePropertiesProvider tablePropertiesProvider;

    public DeadLettersStatusReport(SqsClient sqsClient,
            QueueMessageCount.Client messageCount,
            InstanceProperties instanceProperties,
            TablePropertiesProvider tablePropertiesProvider) {
        this.sqsClient = sqsClient;
        this.messageCount = messageCount;
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
    }

    public void run() {
        System.out.println("\nDead Letters Status Report:\n--------------------------");
        printStats(instanceProperties.get(COMPACTION_JOB_DLQ_URL), "compaction jobs dead-letter", s -> {
            try {
                return new CompactionJobSerDe().fromJson(s).toString();
            } catch (RuntimeException e) {
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
        QueueMessageCount stats = messageCount.getQueueMessageCount(queueUrl);
        System.out.println("Messages on the " + description + " queue:" + stats);

        if (stats.getApproximateNumberOfMessages() > 0) {
            ReceiveMessageResponse response = sqsClient.receiveMessage(request -> request
                    .queueUrl(queueUrl).maxNumberOfMessages(10).visibilityTimeout(1));
            for (Message message : response.messages()) {
                System.out.println(decoder.apply(message.body()));
            }
        }
    }

    public static void main(String[] args) {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        AmazonSQS sqsClientV1 = buildAwsV1Client(AmazonSQSClientBuilder.standard());

        try (SqsClient sqsClient = buildAwsV2Client(SqsClient.builder())) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, args[0]);
            TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDBClient);
            DeadLettersStatusReport statusReport = new DeadLettersStatusReport(
                    sqsClient, QueueMessageCount.withSqsClient(sqsClientV1), instanceProperties, tablePropertiesProvider);
            statusReport.run();
        } finally {
            s3Client.shutdown();
            dynamoDBClient.shutdown();
            sqsClientV1.shutdown();
        }
    }
}
