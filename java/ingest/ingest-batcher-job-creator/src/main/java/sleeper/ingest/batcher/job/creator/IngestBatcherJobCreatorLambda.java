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
package sleeper.ingest.batcher.job.creator;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.ingest.batcher.core.IngestBatcher;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStore;

import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * Lambda function to invoke the batcher to create ingest jobs from files in its store.
 */
@SuppressWarnings("unused")
public class IngestBatcherJobCreatorLambda {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestBatcherJobCreatorLambda.class);

    private final S3Client s3Client;
    private final String configBucket;
    private final SqsClient sqs;
    private final DynamoDbClient dynamoDB;
    private final Supplier<Instant> timeSupplier;
    private final Supplier<String> jobIdSupplier;

    public IngestBatcherJobCreatorLambda() {
        this(S3Client.create(), getConfigBucket(),
                SqsClient.create(), DynamoDbClient.create(),
                Instant::now, () -> UUID.randomUUID().toString());
    }

    public IngestBatcherJobCreatorLambda(
            S3Client s3, String configBucket, SqsClient sqs, DynamoDbClient dynamoDB,
            Supplier<Instant> timeSupplier, Supplier<String> jobIdSupplier) {
        this.s3Client = s3;
        this.configBucket = configBucket;
        this.sqs = sqs;
        this.dynamoDB = dynamoDB;
        this.timeSupplier = timeSupplier;
        this.jobIdSupplier = jobIdSupplier;
    }

    public void eventHandler(ScheduledEvent event, Context context) {
        batchFiles();
    }

    public void batchFiles() {
        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucket);
        LOGGER.info("Loaded instance properties from bucket {}", configBucket);
        TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDB);
        IngestBatcher batcher = IngestBatcher.builder()
                .instanceProperties(instanceProperties)
                .tablePropertiesProvider(tablePropertiesProvider)
                .store(new DynamoDBIngestBatcherStore(dynamoDB, instanceProperties, tablePropertiesProvider))
                .queueClient(new SQSIngestBatcherQueueClient(sqs))
                .timeSupplier(timeSupplier)
                .jobIdSupplier(jobIdSupplier)
                .build();
        batcher.batchFiles();
    }

    private static String getConfigBucket() {
        String s3Bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        if (null == s3Bucket) {
            throw new IllegalArgumentException("Couldn't get S3 bucket from environment variable");
        } else {
            return s3Bucket;
        }
    }
}
