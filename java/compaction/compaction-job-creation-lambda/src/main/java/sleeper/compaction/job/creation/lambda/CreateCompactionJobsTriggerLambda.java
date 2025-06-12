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
package sleeper.compaction.job.creation.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.properties.S3PropertiesReloader;
import sleeper.configurationv2.table.index.DynamoDBTableIndex;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableIndex;
import sleeper.core.util.LoggedDuration;
import sleeper.invoke.tables.InvokeForTables;

import java.time.Instant;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * Creates batches of tables to create compaction jobs for. Sends these batches to an SQS queue to be picked up by
 * {@link CreateCompactionJobsLambda}.
 */
public class CreateCompactionJobsTriggerLambda implements RequestHandler<ScheduledEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateCompactionJobsTriggerLambda.class);

    private final DynamoDbClient dynamoClient = DynamoDbClient.create();
    private final SqsClient sqsClient = SqsClient.create();
    private final InstanceProperties instanceProperties;
    private final PropertiesReloader propertiesReloader;

    public CreateCompactionJobsTriggerLambda() {
        S3Client s3Client = S3Client.create();
        String configBucketName = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucketName);
        propertiesReloader = S3PropertiesReloader.ifConfigured(s3Client, instanceProperties);
    }

    @Override
    public Void handleRequest(ScheduledEvent event, Context context) {
        Instant startTime = Instant.now();
        LOGGER.info("Lambda triggered at {}, started at {}", event.getTime(), startTime);
        propertiesReloader.reloadIfNeeded();

        String queueUrl = instanceProperties.get(COMPACTION_JOB_CREATION_QUEUE_URL);
        TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
        InvokeForTables.sendOneMessagePerTable(sqsClient, queueUrl, tableIndex.streamOnlineTables());

        Instant finishTime = Instant.now();
        LOGGER.info("Lambda finished at {} (ran for {})", finishTime, LoggedDuration.withFullOutput(startTime, finishTime));
        return null;
    }
}
