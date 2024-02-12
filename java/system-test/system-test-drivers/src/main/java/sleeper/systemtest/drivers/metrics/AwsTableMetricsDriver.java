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
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SleeperInstanceContext;
import sleeper.systemtest.dsl.metrics.TableMetricsDriver;
import sleeper.systemtest.dsl.reporting.ReportingContext;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.METRICS_NAMESPACE;

public class AwsTableMetricsDriver implements TableMetricsDriver {

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
        GetMetricDataResponse response = cloudWatch.getMetricData(builder -> builder
                .startTime(reporting.getRecordingStartTime())
                .endTime(Instant.now().plus(Duration.ofHours(1)))
                .metricDataQueries(new TableMetrics(instance)
                        .queryMetricsByMinute(
                                "ActiveFileCount",
                                "RecordCount",
                                "PartitionCount",
                                "LeafPartitionCount",
                                "AverageActiveFilesPerPartition")));
        return response.metricDataResults().stream()
                .collect(toMap(MetricDataResult::id, MetricDataResult::values));
    }

    private static class TableMetrics {
        private final String namespace;
        private final String instanceId;
        private final String tableName;

        private TableMetrics(SleeperInstanceContext instance) {
            InstanceProperties instanceProperties = instance.getInstanceProperties();
            instanceId = instanceProperties.get(ID);
            namespace = instanceProperties.get(METRICS_NAMESPACE);
            tableName = instance.getTableName();
        }

        List<MetricDataQuery> queryMetricsByMinute(String... metricNames) {
            return Stream.of(metricNames)
                    .map(this::queryMetricByMinute)
                    .collect(toUnmodifiableList());
        }

        MetricDataQuery queryMetricByMinute(String metricName) {
            return MetricDataQuery.builder()
                    .id(metricName)
                    .metricStat(metricByMinute(metricName))
                    .build();
        }

        MetricStat metricByMinute(String metricName) {
            return MetricStat.builder()
                    .metric(metric(metricName))
                    .period(60)
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
