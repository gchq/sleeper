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

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.BulkImportJobSerDe;
import sleeper.bulkimport.starter.executor.BulkImportExecutor;
import sleeper.bulkimport.starter.executor.PlatformExecutor;
import sleeper.configuration.properties.PropertiesReloader;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.ingest.job.IngestJobMessageHandler;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.statestore.StateStoreProvider;
import sleeper.utils.HadoopConfigurationProvider;
import sleeper.utils.HadoopPathUtils;

import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * The {@link BulkImportStarterLambda} consumes {@link BulkImportJob} messages from SQS and starts executes them using
 * an {@link BulkImportExecutor}.
 */
public class BulkImportStarterLambda implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportStarterLambda.class);

    private final PropertiesReloader propertiesReloader;
    private final BulkImportExecutor executor;
    private final IngestJobMessageHandler<BulkImportJob> ingestJobMessageHandler;

    public BulkImportStarterLambda() {
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamo = AmazonDynamoDBClientBuilder.defaultClient();
        InstanceProperties instanceProperties = loadInstanceProperties(s3);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3, dynamo);
        PlatformExecutor platformExecutor = PlatformExecutor.fromEnvironment(
                instanceProperties, tablePropertiesProvider);
        Configuration hadoopConfig = HadoopConfigurationProvider.getConfigurationForLambdas(instanceProperties);
        IngestJobStatusStore ingestJobStatusStore = IngestJobStatusStoreFactory.getStatusStore(dynamo, instanceProperties);
        executor = new BulkImportExecutor(instanceProperties, tablePropertiesProvider,
                new StateStoreProvider(dynamo, instanceProperties, hadoopConfig),
                ingestJobStatusStore, s3, platformExecutor, Instant::now);
        propertiesReloader = PropertiesReloader.ifConfigured(s3, instanceProperties, tablePropertiesProvider);
        ingestJobMessageHandler = messageHandler(
                instanceProperties, hadoopConfig, ingestJobStatusStore,
                () -> UUID.randomUUID().toString(), Instant::now);
    }

    public BulkImportStarterLambda(BulkImportExecutor executor, InstanceProperties properties, Configuration hadoopConfig,
                                   IngestJobStatusStore ingestJobStatusStore) {
        this(executor, properties, hadoopConfig, ingestJobStatusStore, () -> UUID.randomUUID().toString(), Instant::now);
    }

    public BulkImportStarterLambda(BulkImportExecutor executor, InstanceProperties properties, Configuration hadoopConfig,
                                   IngestJobStatusStore ingestJobStatusStore, Supplier<String> jobIdSupplier, Supplier<Instant> timeSupplier) {
        this.executor = executor;
        this.propertiesReloader = PropertiesReloader.neverReload();
        this.ingestJobMessageHandler = messageHandler(
                properties, hadoopConfig, ingestJobStatusStore,
                jobIdSupplier, timeSupplier);
    }

    private static IngestJobMessageHandler<BulkImportJob> messageHandler(
            InstanceProperties properties, Configuration hadoopConfig, IngestJobStatusStore ingestJobStatusStore,
            Supplier<String> jobIdSupplier, Supplier<Instant> timeSupplier) {
        return IngestJobMessageHandler.builder().ingestJobStatusStore(ingestJobStatusStore)
                .deserialiser(new BulkImportJobSerDe()::fromJson)
                .toIngestJob(BulkImportJob::toIngestJob)
                .applyIngestJobChanges(BulkImportJob::applyIngestJobChanges)
                .expandDirectories(files -> HadoopPathUtils.expandDirectories(files, hadoopConfig, properties))
                .jobIdSupplier(jobIdSupplier).timeSupplier(timeSupplier)
                .build();
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

    private static InstanceProperties loadInstanceProperties(AmazonS3 s3Client) {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, System.getenv(CONFIG_BUCKET.toEnvironmentVariable()));
        return instanceProperties;
    }
}
