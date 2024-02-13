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

package sleeper.systemtest.drivers.metrics;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.Metric;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataQuery;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataResult;
import software.amazon.awssdk.services.cloudwatch.model.MetricStat;
import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.clients.deploy.InvokeLambda;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.S3TableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.table.TableIdentity;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SleeperInstanceContext;
import sleeper.systemtest.dsl.metrics.TableMetricsDriver;
import sleeper.systemtest.dsl.reporting.ReportingContext;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.METRICS_NAMESPACE;

public class AwsTableMetricsDriver implements TableMetricsDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsTableMetricsDriver.class);
    private static final Map<String, String> METRIC_ID_TO_NAME = Map.of(
            "activeFiles", "ActiveFileCount",
            "records", "RecordCount",
            "partitions", "PartitionCount",
            "leafPartitions", "LeafPartitionCount",
            "filesPerPartition", "AverageActiveFilesPerPartition");

    private final SleeperInstanceContext instance;
    private final ReportingContext reporting;
    private final LambdaClient lambda;
    private final CloudWatchClient cloudWatch;

    public AwsTableMetricsDriver(SleeperInstanceContext instance,
                                 ReportingContext reporting,
                                 SystemTestClients clients) {
        this.instance = instance;
        this.reporting = reporting;
        this.lambda = clients.getLambda();
        this.cloudWatch = clients.getCloudWatch();
    }

    @Override
    public void generateTableMetrics() {
        InvokeLambda.invokeWith(lambda, instance.getInstanceProperties().get(TABLE_METRICS_LAMBDA_FUNCTION));
    }

    @Override
    public Map<String, List<Double>> getTableMetrics() {
        return getTableMetrics(cloudWatch, reporting.getRecordingStartTime(),
                new Dimensions(instance.getInstanceProperties(), instance.getTableProperties()));
    }

    private static Map<String, List<Double>> getTableMetrics(
            CloudWatchClient cloudWatch, Instant startTime, Dimensions dimensions) {
        GetMetricDataResponse response = cloudWatch.getMetricData(builder -> builder
                .startTime(startTime)
                .endTime(startTime.plus(Duration.ofHours(1)))
                .metricDataQueries(dimensions.queryMetricsAverageByHour()));
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
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(s3, instanceId);
        TableProperties tableProperties = S3TableProperties.getStore(instanceProperties, s3, dynamoDB)
                .loadByName(tableName).orElseThrow();
        Dimensions dimensions = new Dimensions(instanceProperties, tableProperties);
        try (CloudWatchClient cloudWatch = CloudWatchClient.create()) {
            LOGGER.info("Found metrics: {}", getTableMetrics(cloudWatch, startTime, dimensions));
        }
    }

    private static class Dimensions {
        private final String namespace;
        private final String instanceId;
        private final String tableName;

        private Dimensions(InstanceProperties instanceProperties, TableProperties tableProperties) {
            instanceId = instanceProperties.get(ID);
            namespace = instanceProperties.get(METRICS_NAMESPACE);
            TableIdentity tableIdentity = tableProperties.getId();
            tableName = tableIdentity.getTableName();
            LOGGER.info("Querying metrics for namespace {}, instance {}, table {}", namespace, instanceId, tableIdentity);
        }

        List<MetricDataQuery> queryMetricsAverageByHour() {
            return METRIC_ID_TO_NAME.entrySet().stream()
                    .map(entry -> queryMetricBy5Minutes(entry.getKey(), entry.getValue()))
                    .collect(toUnmodifiableList());
        }

        MetricDataQuery queryMetricBy5Minutes(String id, String metricName) {
            return MetricDataQuery.builder()
                    .id(id)
                    .metricStat(metricBy5Minutes(metricName))
                    .build();
        }

        MetricStat metricBy5Minutes(String metricName) {
            return MetricStat.builder()
                    .metric(metric(metricName))
                    .stat("Average")
                    .period(5 * 60)
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
                    Dimension.builder().name("tableName").value(tableName).build());
        }
    }
}
