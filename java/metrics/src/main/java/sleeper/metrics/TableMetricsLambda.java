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

package sleeper.metrics;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.cloudwatchlogs.emf.logger.MetricsLogger;
import software.amazon.cloudwatchlogs.emf.model.DimensionSet;
import software.amazon.cloudwatchlogs.emf.model.Unit;
import software.amazon.lambda.powertools.metrics.Metrics;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3PropertiesReloader;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.metrics.TableMetrics;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.table.TableStatus;
import sleeper.core.util.LoggedDuration;
import sleeper.statestore.StateStoreFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static java.util.stream.Collectors.groupingBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.MetricsProperty.METRICS_NAMESPACE;

public class TableMetricsLambda implements RequestHandler<SQSEvent, SQSBatchResponse> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableMetricsLambda.class);

    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final PropertiesReloader propertiesReloader;

    public TableMetricsLambda() {
        S3Client s3Client = S3Client.create();
        DynamoDbClient dynamoClient = DynamoDbClient.create();
        String configBucketName = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucketName);
        tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
        stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient);
        propertiesReloader = S3PropertiesReloader.ifConfigured(s3Client, instanceProperties, tablePropertiesProvider);
    }

    @Override
    @Metrics
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        Instant startTime = Instant.now();
        LOGGER.info("Lambda started at {}", startTime);
        propertiesReloader.reloadIfNeeded();

        Map<String, List<SQSMessage>> messagesByTableId = event.getRecords().stream()
                .collect(groupingBy(SQSEvent.SQSMessage::getBody));
        List<SQSBatchResponse.BatchItemFailure> batchItemFailures = new ArrayList<SQSBatchResponse.BatchItemFailure>();
        List<TableProperties> tables = loadTables(messagesByTableId, batchItemFailures);
        publishStateStoreMetrics(tables, messagesByTableId, batchItemFailures);

        Instant finishTime = Instant.now();
        LOGGER.info("Lambda finished at {} (ran for {})", finishTime, LoggedDuration.withFullOutput(startTime, finishTime));
        return new SQSBatchResponse(batchItemFailures);
    }

    public void publishStateStoreMetrics(
            List<TableProperties> tables,
            Map<String, List<SQSMessage>> messagesByTableId,
            List<SQSBatchResponse.BatchItemFailure> batchItemFailures) {
        String metricsNamespace = instanceProperties.get(METRICS_NAMESPACE);
        LOGGER.info("Generating metrics for namespace {}", metricsNamespace);
        MetricsLogger metricsLogger = Metrics.MetricsUtils.metricsLogger();
        metricsLogger.setNamespace(metricsNamespace);
        for (TableProperties tableProperties : tables) {
            TableStatus tableStatus = tableProperties.getStatus();
            try {
                StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
                TableMetrics metrics = TableMetrics.from(
                        instanceProperties.get(ID), tableStatus, stateStore);
                publishStateStoreMetrics(metricsLogger, metrics);
            } catch (RuntimeException e) {
                LOGGER.error("Failed publishing metrics for table {}", tableStatus, e);
                messagesByTableId.get(tableStatus.getTableUniqueId()).stream()
                        .map(SQSMessage::getMessageId)
                        .map(SQSBatchResponse.BatchItemFailure::new)
                        .forEach(batchItemFailures::add);
            }
        }
    }

    public void publishStateStoreMetrics(MetricsLogger metricsLogger, TableMetrics metrics) {
        metricsLogger.setDimensions(DimensionSet.of(
                "instanceId", metrics.getInstanceId(),
                "tableName", metrics.getTableName()));

        metricsLogger.putMetric("NumberOfFilesWithReferences", metrics.getFileCount(), Unit.COUNT);
        metricsLogger.putMetric("RecordCount", metrics.getRecordCount(), Unit.COUNT);
        metricsLogger.putMetric("PartitionCount", metrics.getPartitionCount(), Unit.COUNT);
        metricsLogger.putMetric("LeafPartitionCount", metrics.getLeafPartitionCount(), Unit.COUNT);
        // TODO: Work out how to publish min and max active files per partition too
        // This is possible via the CloudMetrics API by publishing a statistic set (https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/publishingMetrics.html#publishingDataPoints1)
        // Is it possible when publishing via the embedded metric format though?
        metricsLogger.putMetric("AverageFileReferencesPerPartition", metrics.getAverageFileReferencesPerPartition(), Unit.COUNT);
        metricsLogger.setTimestamp(Instant.now());
        metricsLogger.flush();
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
