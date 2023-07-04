/*
 * Copyright 2022-2023 Crown Copyright
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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.ingest.batcher.IngestBatcher;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * A lambda function to create jobs with the {@link IngestBatcher}.
 */
@SuppressWarnings("unused")
public class IngestBatcherJobCreatorLambda {
    private final AmazonS3 s3Client;
    private final String configBucket;

    private final AmazonSQS sqs;
    private final AmazonDynamoDB dynamoDB;
    private final Supplier<Instant> timeSupplier;
    private final Supplier<String> jobIdSupplier;

    public IngestBatcherJobCreatorLambda() {
        this(AmazonS3ClientBuilder.defaultClient(), getConfigBucket(),
                AmazonSQSClientBuilder.defaultClient(), AmazonDynamoDBClientBuilder.defaultClient(),
                Instant::now, () -> UUID.randomUUID().toString());
    }

    public IngestBatcherJobCreatorLambda(AmazonS3 s3, String configBucket,
                                         AmazonSQS sqs, AmazonDynamoDB dynamoDB,
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
        InstanceProperties instanceProperties = new InstanceProperties();
        try {
            instanceProperties.loadFromS3(s3Client, configBucket);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        IngestBatcher batcher = IngestBatcher.builder()
                .instanceProperties(instanceProperties)
                .tablePropertiesProvider(tablePropertiesProvider)
                .store(new DynamoDBIngestBatcherStore(dynamoDB, instanceProperties, tablePropertiesProvider))
                .queueClient(new SQSIngestBatcherQueueClient(sqs))
                .timeSupplier(timeSupplier).jobIdSupplier(jobIdSupplier)
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
