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

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.partition.Partition;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.stream.Collectors;

public class TableMetricsLambda implements RequestHandler<String, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableMetricsLambda.class);

    private AmazonS3 s3Client;
    private AmazonDynamoDB dynamoClient;

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
    public Void handleRequest(String input, Context context) {
        LOGGER.info("Received event: {}", input);

        String[] config = input.split("\\|");
        String configBucketName = config[0];
        String tableName = config[1];
        LOGGER.info("Config bucket is {}, table name is {}", configBucketName, tableName);

        try {
            publishStateStoreMetrics(configBucketName, tableName);
        } catch (Exception e) {
            LOGGER.error("{}", e);
        }

        return null;
    }

    public void publishStateStoreMetrics(String configBucketName, String tableName) throws IOException, StateStoreException {
        LOGGER.info("Loading instance properties from config bucket {}", configBucketName);
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, configBucketName);

        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoClient, instanceProperties, new Configuration());
        StateStore stateStore = stateStoreProvider.getStateStore(tableName, tablePropertiesProvider);

        LOGGER.info("Querying state store for table {} for active files", tableName);
        List<FileInfo> fileInPartitions = stateStore.getFileInPartitionList();
        LOGGER.info("Found {} file in partition records for table {}", fileInPartitions.size(), tableName);
        int fileCount = fileInPartitions.size();
        long recordCount = fileInPartitions.stream().mapToLong(activeFile -> activeFile.getNumberOfRecords()).sum();
        LOGGER.info("Total number of records in table {} is {}", tableName, recordCount);

        LongSummaryStatistics filesPerPartitionStats = fileInPartitions.stream().collect(
                Collectors.groupingBy(
                        activeFile -> activeFile.getPartitionId(),
                        Collectors.counting()
                )
        ).values().stream().mapToLong(value -> value).summaryStatistics();
        LOGGER.info("{}", filesPerPartitionStats);

        LOGGER.info("Querying state store for table {} for partitions", tableName);
        List<Partition> partitions = stateStore.getAllPartitions();
        int partitionCount = partitions.size();
        long leafPartitionCount = partitions.stream().filter(Partition::isLeafPartition).count();
        LOGGER.info("Found {} partitions and {} leaf partitions for table {}", partitionCount, leafPartitionCount, tableName);

        String metricsNamespace = instanceProperties.get(UserDefinedInstanceProperty.METRICS_NAMESPACE);
        LOGGER.info("Generating metrics for namespace {}", metricsNamespace);
        MetricsLogger metricsLogger = MetricsUtils.metricsLogger();
        metricsLogger.setNamespace(metricsNamespace);
        metricsLogger.setDimensions(DimensionSet.of(
                "instanceId", instanceProperties.get(UserDefinedInstanceProperty.ID),
                "tableName", tableName
        ));

        metricsLogger.putMetric("ActiveFileCount", fileCount, Unit.COUNT);
        metricsLogger.putMetric("RecordCount", recordCount, Unit.COUNT);
        metricsLogger.putMetric("PartitionCount", partitionCount, Unit.COUNT);
        metricsLogger.putMetric("LeafPartitionCount", leafPartitionCount, Unit.COUNT);
        // TODO: Work out how to publish min and max active files per partition too
        // This is possible via the CloudMetrics API by publishing a statistic set (https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/publishingMetrics.html#publishingDataPoints1)
        // Is it possible when publishing via the embedded metric format though?
        metricsLogger.putMetric("AverageActiveFilesPerPartition", filesPerPartitionStats.getAverage(), Unit.COUNT);
        metricsLogger.flush();
    }

    public static void main(String[] args) throws IOException, StateStoreException {
        if (args.length != 2) {
            throw new RuntimeException("Syntax: " + TableMetricsLambda.class.getSimpleName() + " <configBucketName> <tableName>");
        }

        AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.defaultClient();
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

        TableMetricsLambda lambda = new TableMetricsLambda(s3Client, dynamoClient);
        lambda.publishStateStoreMetrics(args[0], args[1]);
    }
}
