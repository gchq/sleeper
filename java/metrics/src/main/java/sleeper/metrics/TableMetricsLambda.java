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

package sleeper.metrics;

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
import software.amazon.cloudwatchlogs.emf.logger.MetricsLogger;
import software.amazon.cloudwatchlogs.emf.model.DimensionSet;
import software.amazon.cloudwatchlogs.emf.model.Unit;
import software.amazon.lambda.powertools.metrics.MetricsUtils;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.LoggedDuration;
import sleeper.statestore.StateStoreProvider;

import java.time.Instant;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.METRICS_NAMESPACE;

@SuppressWarnings("unused")
public class TableMetricsLambda implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableMetricsLambda.class);

    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoClient;
    private final String configBucketName;
    private final CalculateTableMetricsSerDe serDe = new CalculateTableMetricsSerDe();

    public TableMetricsLambda() {
        this(
                AmazonS3ClientBuilder.defaultClient(),
                AmazonDynamoDBClientBuilder.defaultClient(),
                System.getenv(CONFIG_BUCKET.toEnvironmentVariable()));
    }

    public TableMetricsLambda(AmazonS3 s3Client, AmazonDynamoDB dynamoClient, String configBucketName) {
        this.s3Client = s3Client;
        this.dynamoClient = dynamoClient;
        this.configBucketName = configBucketName;
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        Instant startTime = Instant.now();
        LOGGER.info("Lambda started at {}", startTime);

        event.getRecords().stream()
                .map(SQSEvent.SQSMessage::getBody)
                .peek(body -> LOGGER.info("Received message: {}", body))
                .map(serDe::fromJson)
                .forEach(this::publishStateStoreMetrics);

        Instant finishTime = Instant.now();
        LOGGER.info("Lambda finished at {} (ran for {})", finishTime, LoggedDuration.withFullOutput(startTime, finishTime));
        return null;
    }

    public void publishStateStoreMetrics(CalculateTableMetricsRequest request) {
        LOGGER.info("Loading instance properties from config bucket {}", configBucketName);
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, configBucketName);
        String metricsNamespace = instanceProperties.get(METRICS_NAMESPACE);
        LOGGER.info("Generating metrics for namespace {}", metricsNamespace);
        MetricsLogger metricsLogger = MetricsUtils.metricsLogger();
        metricsLogger.setNamespace(metricsNamespace);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoClient);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoClient, instanceProperties, new Configuration());
        for (String tableId : request.getTableIds()) {
            try {
                TableProperties tableProperties = tablePropertiesProvider.getById(tableId);
                StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
                TableMetrics metrics = TableMetrics.from(instanceProperties, tableProperties, stateStore);
                publishStateStoreMetrics(metricsLogger, metrics);
            } catch (Exception e) {
                LOGGER.error("Failed publishing metrics for table {}", tableId, e);
            }
        }
    }

    public void publishStateStoreMetrics(MetricsLogger metricsLogger, TableMetrics metrics) {
        metricsLogger.setDimensions(DimensionSet.of(
                "instanceId", metrics.getInstanceId(),
                "tableName", metrics.getTableName()
        ));

        metricsLogger.putMetric("ActiveFileCount", metrics.getFileCount(), Unit.COUNT);
        metricsLogger.putMetric("RecordCount", metrics.getRecordCount(), Unit.COUNT);
        metricsLogger.putMetric("PartitionCount", metrics.getPartitionCount(), Unit.COUNT);
        metricsLogger.putMetric("LeafPartitionCount", metrics.getLeafPartitionCount(), Unit.COUNT);
        // TODO: Work out how to publish min and max active files per partition too
        // This is possible via the CloudMetrics API by publishing a statistic set (https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/publishingMetrics.html#publishingDataPoints1)
        // Is it possible when publishing via the embedded metric format though?
        metricsLogger.putMetric("AverageActiveFilesPerPartition", metrics.getAverageActiveFilesPerPartition(), Unit.COUNT);
        metricsLogger.flush();
    }
}
