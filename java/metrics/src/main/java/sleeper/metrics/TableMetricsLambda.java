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
package sleeper.metrics;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.cloudwatchlogs.emf.logger.MetricsLogger;
import software.amazon.cloudwatchlogs.emf.model.DimensionSet;
import software.amazon.cloudwatchlogs.emf.model.Unit;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.metrics.MetricsUtils;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.statestore.StateStoreProvider;

import java.util.List;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.instance.CommonProperty.METRICS_NAMESPACE;

public class TableMetricsLambda implements RequestHandler<String, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableMetricsLambda.class);

    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoClient;

    public TableMetricsLambda() {
        this(
                AmazonS3ClientBuilder.defaultClient(),
                AmazonDynamoDBClientBuilder.defaultClient()
        );
    }

    public TableMetricsLambda(AmazonS3 s3Client, AmazonDynamoDB dynamoClient) {
        this.s3Client = s3Client;
        this.dynamoClient = dynamoClient;
    }

    @Override
    @Metrics
    public Void handleRequest(String configBucketName, Context context) {
        LOGGER.info("Received event for config bucket: {}", configBucketName);

        try {
            publishStateStoreMetrics(configBucketName);
        } catch (Exception e) {
            LOGGER.error("Failed publishing metrics", e);
        }

        return null;
    }

    public void publishStateStoreMetrics(String configBucketName) {
        LOGGER.info("Loading instance properties from config bucket {}", configBucketName);
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, configBucketName);

        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoClient);
        List<TableProperties> tablePropertiesList = tablePropertiesProvider.streamAllTables().collect(Collectors.toUnmodifiableList());
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoClient, instanceProperties, new Configuration());

        String metricsNamespace = instanceProperties.get(METRICS_NAMESPACE);
        LOGGER.info("Generating metrics for namespace {}", metricsNamespace);
        MetricsLogger metricsLogger = MetricsUtils.metricsLogger();
        metricsLogger.setNamespace(metricsNamespace);

        TableMetrics.streamFrom(instanceProperties, tablePropertiesList, stateStoreProvider).forEach(metrics -> {
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
        });
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            throw new RuntimeException("Syntax: " + TableMetricsLambda.class.getSimpleName() + " <configBucketName>");
        }

        AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.defaultClient();
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

        TableMetricsLambda lambda = new TableMetricsLambda(s3Client, dynamoClient);
        lambda.publishStateStoreMetrics(args[0]);
    }
}
