/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.query.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.query.core.model.QuerySerDe;
import sleeper.query.runner.tracker.DynamoDBQueryTracker;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_DLQ_URL;

/**
 * Handles queries whose processing lambda failed without reporting the failure (for example, when the
 * lambda was killed by an execution timeout). Such queries are redirected here by the query queue's
 * dead letter configuration. This lambda ensures each query is marked as failed in the tracker, then
 * forwards it to the dead letter queue.
 */
public class SqsQueryFailureLambda implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsQueryFailureLambda.class);

    private final SqsClient sqsClient;
    private final QuerySerDe querySerDe;
    private final QueryFailureProcessor processor;
    private final String deadLetterQueueUrl;

    public SqsQueryFailureLambda() {
        this(buildAwsV2Client(S3Client.builder()),
                buildAwsV2Client(SqsClient.builder()),
                buildAwsV2Client(DynamoDbClient.builder()),
                System.getenv(CONFIG_BUCKET.toEnvironmentVariable()));
    }

    public SqsQueryFailureLambda(S3Client s3Client, SqsClient sqsClient, DynamoDbClient dynamoClient, String configBucket) {
        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucket);
        TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
        this.sqsClient = sqsClient;
        this.querySerDe = new QuerySerDe(tablePropertiesProvider);
        DynamoDBQueryTracker tracker = new DynamoDBQueryTracker(instanceProperties, dynamoClient);
        this.processor = new QueryFailureProcessor(tracker, tracker);
        this.deadLetterQueueUrl = instanceProperties.get(LEAF_PARTITION_QUERY_QUEUE_DLQ_URL);
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        for (SQSEvent.SQSMessage message : event.getRecords()) {
            String body = message.getBody();
            LOGGER.info("Received failed query message with body {}", body);

            try {
                processor.queryFailed(querySerDe.fromJsonOrLeafQuery(body).asLeafQuery());
            } catch (RuntimeException e) {
                LOGGER.error("Failed to mark query as failed for message {}", body, e);
            }

            sqsClient.sendMessage(send -> send.queueUrl(deadLetterQueueUrl).messageBody(body));
        }

        return null;
    }

}
