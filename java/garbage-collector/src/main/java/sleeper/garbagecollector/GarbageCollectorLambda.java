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
package sleeper.garbagecollector;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3PropertiesReloader;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.table.TableStatus;
import sleeper.core.util.LoggedDuration;
import sleeper.garbagecollector.FailedGarbageCollectionException.TableFailures;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static java.util.stream.Collectors.groupingBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.garbagecollector.GarbageCollector.deleteFileAndSketches;
import static sleeper.garbagecollector.GarbageCollector.sendAsyncCommit;

/**
 * Runs the garbage collector in AWS Lambda. Builds and invokes {@link GarbageCollector} for a batch of tables.
 */
public class GarbageCollectorLambda implements RequestHandler<SQSEvent, SQSBatchResponse> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GarbageCollectorLambda.class);

    private final TablePropertiesProvider tablePropertiesProvider;
    private final PropertiesReloader propertiesReloader;
    private final GarbageCollector garbageCollector;

    public GarbageCollectorLambda() {
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();

        String s3Bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        if (null == s3Bucket) {
            throw new RuntimeException("Couldn't get S3 bucket from environment variable");
        }

        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, s3Bucket);
        LOGGER.debug("Loaded InstanceProperties from {}", s3Bucket);

        tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDBClient);
        propertiesReloader = S3PropertiesReloader.ifConfigured(s3Client, instanceProperties, tablePropertiesProvider);
        Configuration conf = HadoopConfigurationProvider.getConfigurationForLambdas(instanceProperties);
        StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoDBClient, conf);
        garbageCollector = new GarbageCollector(deleteFileAndSketches(conf),
                instanceProperties, stateStoreProvider,
                sendAsyncCommit(instanceProperties, sqsClient));
    }

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        Instant startTime = Instant.now();
        LOGGER.info("Lambda started at {}", startTime);
        propertiesReloader.reloadIfNeeded();

        Map<String, List<SQSMessage>> messagesByTableId = event.getRecords().stream()
                .collect(groupingBy(SQSEvent.SQSMessage::getBody));
        List<SQSBatchResponse.BatchItemFailure> batchItemFailures = new ArrayList<SQSBatchResponse.BatchItemFailure>();
        List<TableProperties> tables = loadTables(messagesByTableId, batchItemFailures);
        try {
            garbageCollector.run(tables);
        } catch (FailedGarbageCollectionException e) {
            LOGGER.error("Found {} tables with failures", e.getTableFailures().size(), e);
            e.getTableFailures().stream()
                    .map(TableFailures::getTable)
                    .map(TableStatus::getTableUniqueId)
                    .flatMap(tableId -> messagesByTableId.get(tableId).stream())
                    .map(SQSMessage::getMessageId)
                    .map(SQSBatchResponse.BatchItemFailure::new)
                    .forEach(batchItemFailures::add);
        } finally {
            Instant finishTime = Instant.now();
            LOGGER.info("Lambda finished at {} (ran for {}), {} failures",
                    finishTime, LoggedDuration.withFullOutput(startTime, finishTime), batchItemFailures.size());
        }
        return new SQSBatchResponse(batchItemFailures);
    }

    private List<TableProperties> loadTables(
            Map<String, List<SQSMessage>> messagesByTableId,
            List<SQSBatchResponse.BatchItemFailure> batchItemFailures) {
        List<TableProperties> tables = new ArrayList<>();
        for (Entry<String, List<SQSMessage>> tableAndMessages : messagesByTableId.entrySet()) {
            String tableId = tableAndMessages.getKey();
            List<SQSMessage> tableMessages = tableAndMessages.getValue();
            try {
                TableProperties tableProperties = tablePropertiesProvider.getById(tableId);
                LOGGER.info("Received {} messages for table {}", tableMessages.size(), tableProperties.getStatus());
                tables.add(tableProperties);
            } catch (RuntimeException e) {
                LOGGER.error("Failed loading properties for table {}", tableId, e);
                tableMessages.stream()
                        .map(SQSMessage::getMessageId)
                        .map(SQSBatchResponse.BatchItemFailure::new)
                        .forEach(batchItemFailures::add);
            }
        }
        return tables;
    }
}
