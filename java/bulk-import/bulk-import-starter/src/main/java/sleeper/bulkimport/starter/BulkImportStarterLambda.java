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
package sleeper.bulkimport.starter;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.core.job.BulkImportJobSerDe;
import sleeper.bulkimport.starter.executor.BulkImportExecutor;
import sleeper.bulkimport.starter.executor.BulkImportJobWriterToS3;
import sleeper.bulkimport.starter.executor.PlatformExecutor;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3PropertiesReloader;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.ingest.core.job.IngestJobMessageHandler;
import sleeper.ingest.status.store.job.IngestJobTrackerFactory;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.parquet.utils.HadoopPathUtils;
import sleeper.statestore.StateStoreFactory;

import java.time.Instant;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * Consumes bulk import jobs from SQS and starts them in the execution platform. An environment variable configures
 * which platform bulk import jobs will be executed on, using an instance of {@link PlatformExecutor}.
 */
public class BulkImportStarterLambda implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportStarterLambda.class);

    private final PropertiesReloader propertiesReloader;
    private final BulkImportExecutor executor;
    private final IngestJobMessageHandler<BulkImportJob> ingestJobMessageHandler;

    public BulkImportStarterLambda() {
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamo = AmazonDynamoDBClientBuilder.defaultClient();
        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3, System.getenv(CONFIG_BUCKET.toEnvironmentVariable()));
        TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3, dynamo);
        PlatformExecutor platformExecutor = PlatformExecutor.fromEnvironment(
                instanceProperties, tablePropertiesProvider);
        Configuration hadoopConfig = HadoopConfigurationProvider.getConfigurationForLambdas(instanceProperties);
        IngestJobTracker ingestJobTracker = IngestJobTrackerFactory.getTracker(dynamo, instanceProperties);
        executor = new BulkImportExecutor(instanceProperties, tablePropertiesProvider,
                StateStoreFactory.createProvider(instanceProperties, s3, dynamo, hadoopConfig),
                ingestJobTracker, new BulkImportJobWriterToS3(instanceProperties, s3),
                platformExecutor, Instant::now);
        propertiesReloader = S3PropertiesReloader.ifConfigured(s3, instanceProperties, tablePropertiesProvider);
        ingestJobMessageHandler = messageHandlerBuilder()
                .tableIndex(new DynamoDBTableIndex(instanceProperties, dynamo))
                .ingestJobTracker(ingestJobTracker)
                .expandDirectories(files -> HadoopPathUtils.expandDirectories(files, hadoopConfig, instanceProperties))
                .build();
    }

    public BulkImportStarterLambda(BulkImportExecutor executor, IngestJobMessageHandler<BulkImportJob> messageHandler) {
        this.executor = executor;
        this.propertiesReloader = PropertiesReloader.neverReload();
        this.ingestJobMessageHandler = messageHandler;
    }

    public static IngestJobMessageHandler.Builder<BulkImportJob> messageHandlerBuilder() {
        return IngestJobMessageHandler.builder()
                .deserialiser(new BulkImportJobSerDe()::fromJson)
                .toIngestJob(BulkImportJob::toIngestJob)
                .applyIngestJobChanges(BulkImportJob::applyIngestJobChanges);
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        LOGGER.info("Received request: {}", event);
        propertiesReloader.reloadIfNeeded();
        event.getRecords().stream()
                .map(SQSEvent.SQSMessage::getBody)
                .flatMap(message -> ingestJobMessageHandler.deserialiseAndValidate(message).stream())
                .forEach(executor::runJob);
        return null;
    }
}
