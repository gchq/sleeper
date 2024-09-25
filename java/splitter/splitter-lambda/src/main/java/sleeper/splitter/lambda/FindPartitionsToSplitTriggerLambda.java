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

import sleeper.configuration.s3properties.PropertiesReloader;
import sleeper.configuration.s3properties.S3InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableIndex;
import sleeper.core.util.LoggedDuration;
import sleeper.invoke.tables.InvokeForTables;

import java.time.Instant;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.FIND_PARTITIONS_TO_SPLIT_QUEUE_URL;

/**
 * A lambda to invoke partition splitting with batches of tables.
 */
public class FindPartitionsToSplitTriggerLambda implements RequestHandler<ScheduledEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FindPartitionsToSplitTriggerLambda.class);

    private final AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.defaultClient();
    private final AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
    private final InstanceProperties instanceProperties;
    private final PropertiesReloader propertiesReloader;

    public FindPartitionsToSplitTriggerLambda() {
        String configBucketName = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucketName);
        propertiesReloader = PropertiesReloader.ifConfigured(s3Client, instanceProperties);
    }

    @Override
    public Void handleRequest(ScheduledEvent event, Context context) {
        Instant startTime = Instant.now();
        LOGGER.info("Lambda triggered at {}, started at {}", event.getTime(), startTime);
        propertiesReloader.reloadIfNeeded();

        String queueUrl = instanceProperties.get(FIND_PARTITIONS_TO_SPLIT_QUEUE_URL);
        TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
        InvokeForTables.sendOneMessagePerTable(sqsClient, queueUrl, tableIndex.streamOnlineTables());

        Instant finishTime = Instant.now();
        LOGGER.info("Lambda finished at {} (ran for {})", finishTime, LoggedDuration.withFullOutput(startTime, finishTime));
        return null;
    }
}
