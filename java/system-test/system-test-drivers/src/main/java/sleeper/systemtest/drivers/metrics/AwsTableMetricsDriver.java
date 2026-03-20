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

package sleeper.systemtest.drivers.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.Metric;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataQuery;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataResult;
import software.amazon.awssdk.services.cloudwatch.model.MetricStat;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.metrics.TableMetrics;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.table.TableStatus;
import sleeper.core.util.PollWithRetries;
import sleeper.invoke.tables.InvokeForTables;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.metrics.TableMetricsDriver;
import sleeper.systemtest.dsl.reporting.ReportingContext;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.MetricsProperty.METRICS_NAMESPACE;

public class AwsTableMetricsDriver implements TableMetricsDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsTableMetricsDriver.class);
    private static final Map<String, String> METRIC_ID_TO_NAME = Map.of(
            "filesWithReferences", "NumberOfFilesWithReferences",
            "rows", "RowCount",
            "partitions", "PartitionCount",
            "leafPartitions", "LeafPartitionCount",
            "filesReferencesPerPartition", "AverageFileReferencesPerPartition");
    private static final String QUERY_METRIC_STATISTIC = "Average";
    private static final int QUERY_METRIC_PERIOD_SECONDS = 5 * 60;

    private final SystemTestInstanceContext instance;
    private final ReportingContext reporting;
    private final SqsClient sqsClient;
    private final CloudWatchClient cloudWatch;

    public AwsTableMetricsDriver(SystemTestInstanceContext instance,
            ReportingContext reporting,
            SystemTestClients clients) {
        this.instance = instance;
        this.reporting = reporting;
        this.sqsClient = clients.getSqs();
        this.cloudWatch = clients.getCloudWatch();
    }

    @Override
    public void generateTableMetrics() {
        String queueUrl = instance.getInstanceProperties().get(TABLE_METRICS_QUEUE_URL);
        InvokeForTables.sendOneMessagePerTable(sqsClient, queueUrl, instance.streamTableProperties().map(TableProperties::getStatus));
    }

    @Override
    public TableMetrics getTableMetrics() {
        Dimensions dimensions = new Dimensions(instance);
        Map<String, List<Double>> map = pollTableMetrics(dimensions);
        return TableMetrics.builder()
                .instanceId(dimensions.instanceId)
                .tableName(dimensions.table.getTableName())
                .fileCount((int) getMetric(map, "filesWithReferences"))
                .rowCount((long) getMetric(map, "rows"))
                .partitionCount((int) getMetric(map, "partitions"))
                .leafPartitionCount((int) getMetric(map, "leafPartitions"))
                .averageFileReferencesPerPartition(getMetric(map, "filesReferencesPerPartition"))
                .build();
    }

    private double getMetric(Map<String, List<Double>> map, String id) {
        String name = METRIC_ID_TO_NAME.get(id);
        List<Double> values = map.get(name);
        int size = values.size();
        if (size != 1) {
            throw new RuntimeException("Expected 1 value, found " + size + " for metric " + name);
        }
        return values.get(0);
    }

    private Map<String, List<Double>> pollTableMetrics(Dimensions dimensions) {
        Instant startTime = reporting.getRecordingStartTime();
        try {
            // Metrics can take a few seconds to show up in CloudWatch, so poll if it's not there yet
            return PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(5), Duration.ofMinutes(5))
                    .queryUntil("metrics found", () -> getTableMetrics(cloudWatch, startTime, dimensions),
                            results -> results.values().stream().noneMatch(List::isEmpty));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static Map<String, List<Double>> getTableMetrics(
            CloudWatchClient cloudWatch, Instant startTime, Dimensions dimensions) {
        // CloudWatch needs the start time truncated to the minute
        Instant truncatedStartTime = startTime.truncatedTo(ChronoUnit.MINUTES).minus(Duration.ofMinutes(1));
        LOGGER.info("Querying metrics for namespace {}, instance {}, table {}, starting at time: {}",
                dimensions.namespace, dimensions.instanceId, dimensions.table, truncatedStartTime);
        GetMetricDataResponse response = cloudWatch.getMetricData(builder -> builder
                .startTime(truncatedStartTime)
                .endTime(truncatedStartTime.plus(Duration.ofHours(1)))
                .metricDataQueries(dimensions.queryAllMetrics()));
        LOGGER.info("Found metric data: {}", response);
        return response.metricDataResults().stream()
                .collect(toMap(
                        result -> METRIC_ID_TO_NAME.get(result.id()),
                        MetricDataResult::values));
    }

    public static void main(String[] args) {
        String instanceId = args[0];
        String tableName = args[1];
        Instant startTime = Instant.parse(args[2]);
        Dimensions dimensions;
        try (S3Client s3Client = S3Client.create();
                DynamoDbClient dynamoClient = DynamoDbClient.create()) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TableProperties tableProperties = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient)
                    .loadByName(tableName);
            dimensions = new Dimensions(instanceProperties, tableProperties);
        }
        try (CloudWatchClient cloudWatch = CloudWatchClient.create()) {
            LOGGER.info("Found metrics: {}", getTableMetrics(cloudWatch, startTime, dimensions));
        }
    }

    private static class Dimensions {
        private final String namespace;
        private final String instanceId;
        private final TableStatus table;

        private Dimensions(SystemTestInstanceContext instance) {
            this(instance.getInstanceProperties(), instance.getTableProperties());
        }

        private Dimensions(InstanceProperties instanceProperties, TableProperties tableProperties) {
            instanceId = instanceProperties.get(ID);
            namespace = instanceProperties.get(METRICS_NAMESPACE);
            table = tableProperties.getStatus();
        }

        List<MetricDataQuery> queryAllMetrics() {
            return METRIC_ID_TO_NAME.entrySet().stream()
                    .map(entry -> metricDataQuery(entry.getKey(), entry.getValue()))
                    .collect(toUnmodifiableList());
        }

        MetricDataQuery metricDataQuery(String id, String metricName) {
            return MetricDataQuery.builder()
                    .id(id)
                    .metricStat(metricStat(metricName))
                    .build();
        }

        MetricStat metricStat(String metricName) {
            return MetricStat.builder()
                    .metric(metric(metricName))
                    .stat(QUERY_METRIC_STATISTIC)
                    .period(QUERY_METRIC_PERIOD_SECONDS)
                    .build();
        }

        Metric metric(String metricName) {
            return Metric.builder()
                    .namespace(namespace)
                    .metricName(metricName)
                    .dimensions(dimensions())
                    .build();
        }

        Collection<Dimension> dimensions() {
            return List.of(
                    Dimension.builder().name("instanceId").value(instanceId).build(),
                    Dimension.builder().name("tableName").value(table.getTableName()).build());
        }
    }
}
