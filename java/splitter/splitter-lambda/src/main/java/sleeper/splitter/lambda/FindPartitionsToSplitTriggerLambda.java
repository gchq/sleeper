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
package sleeper.splitter.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.table.InvokeForTableRequest;
import sleeper.core.table.InvokeForTableRequestSerDe;
import sleeper.core.table.TableIndex;
import sleeper.core.util.LoggedDuration;

import java.time.Instant;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_TABLE_BATCH_QUEUE_URL;
import static sleeper.configuration.properties.instance.PartitionSplittingProperty.PARTITION_SPLITTING_TABLE_BATCH_SIZE;

/**
 * A lambda to invoke partition splitting with batches of tables.
 */
public class FindPartitionsToSplitTriggerLambda implements RequestHandler<ScheduledEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FindPartitionsToSplitTriggerLambda.class);

    private final InstanceProperties instanceProperties = new InstanceProperties();
    private final InvokeForTableRequestSerDe serDe = new InvokeForTableRequestSerDe();
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoClient;
    private final AmazonSQS sqsClient;
    private final String configBucketName;

    public FindPartitionsToSplitTriggerLambda() {
        this.s3Client = AmazonS3ClientBuilder.defaultClient();
        this.dynamoClient = AmazonDynamoDBClientBuilder.defaultClient();
        this.sqsClient = AmazonSQSClientBuilder.defaultClient();
        this.configBucketName = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
    }

    @Override
    public Void handleRequest(ScheduledEvent event, Context context) {
        Instant startTime = Instant.now();
        LOGGER.info("Lambda triggered at {}, started at {}", event.getTime(), startTime);
        instanceProperties.loadFromS3(s3Client, configBucketName);
        int batchSize = instanceProperties.getInt(PARTITION_SPLITTING_TABLE_BATCH_SIZE);
        String queueUrl = instanceProperties.get(PARTITION_SPLITTING_TABLE_BATCH_QUEUE_URL);
        TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
        InvokeForTableRequest.forTables(tableIndex.streamOnlineTables(), batchSize,
                request -> sqsClient.sendMessage(queueUrl, serDe.toJson(request)));

        Instant finishTime = Instant.now();
        LOGGER.info("Lambda finished at {} (ran for {})", finishTime, LoggedDuration.withFullOutput(startTime, finishTime));
        return null;
    }

}
