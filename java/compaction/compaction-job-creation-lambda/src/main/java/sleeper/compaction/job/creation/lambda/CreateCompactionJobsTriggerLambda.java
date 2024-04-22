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
package sleeper.compaction.job.creation.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatus;
import sleeper.core.util.LoggedDuration;
import sleeper.core.util.SplitIntoBatches;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * Creates batches of tables to create compaction jobs for. Sends these batches to an SQS queue to be picked up by
 * {@link CreateCompactionJobsLambda}.
 */
public class CreateCompactionJobsTriggerLambda implements RequestHandler<ScheduledEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateCompactionJobsTriggerLambda.class);

    private final InstanceProperties instanceProperties = new InstanceProperties();
    private final AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.defaultClient();
    private final AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();

    public CreateCompactionJobsTriggerLambda() {
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        String configBucketName = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        instanceProperties.loadFromS3(s3Client, configBucketName);
    }

    @Override
    public Void handleRequest(ScheduledEvent event, Context context) {
        Instant startTime = Instant.now();
        LOGGER.info("Lambda triggered at {}, started at {}", event.getTime(), startTime);
        String queueUrl = instanceProperties.get(COMPACTION_JOB_CREATION_QUEUE_URL);
        TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
        SplitIntoBatches.reusingListOfSize(10,
                tableIndex.streamOnlineTables(),
                tables -> sendMessageBatch(tables, queueUrl));

        Instant finishTime = Instant.now();
        LOGGER.info("Lambda finished at {} (ran for {})", finishTime, LoggedDuration.withFullOutput(startTime, finishTime));
        return null;
    }

    private void sendMessageBatch(List<TableStatus> tables, String queueUrl) {
        sqsClient.sendMessageBatch(new SendMessageBatchRequest()
                .withQueueUrl(queueUrl)
                .withEntries(tables.stream()
                        .map(table -> new SendMessageBatchRequestEntry()
                                .withMessageDeduplicationId(UUID.randomUUID().toString())
                                .withMessageGroupId(table.getTableUniqueId())
                                .withMessageBody(table.getTableUniqueId()))
                        .collect(toUnmodifiableList())));
    }

}
