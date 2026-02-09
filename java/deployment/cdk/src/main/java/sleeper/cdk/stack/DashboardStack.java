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
package sleeper.cdk.stack;

import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.cloudwatch.Dashboard;
import software.amazon.awscdk.services.cloudwatch.GraphWidget;
import software.amazon.awscdk.services.cloudwatch.GraphWidgetView;
import software.amazon.awscdk.services.cloudwatch.IMetric;
import software.amazon.awscdk.services.cloudwatch.MathExpression;
import software.amazon.awscdk.services.cloudwatch.Metric;
import software.amazon.awscdk.services.cloudwatch.MetricOptions;
import software.amazon.awscdk.services.cloudwatch.SingleValueWidget;
import software.amazon.awscdk.services.cloudwatch.TextWidget;
import software.amazon.awscdk.services.cloudwatch.Unit;
import software.amazon.awscdk.services.cloudwatch.YAxisProps;
import software.constructs.Construct;

import sleeper.cdk.stack.compaction.CompactionStack;
import sleeper.cdk.stack.ingest.IngestStack;
import sleeper.cdk.util.Utils;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.MetricsProperty.DASHBOARD_TIME_WINDOW_MINUTES;
import static sleeper.core.properties.instance.MetricsProperty.METRICS_NAMESPACE;

public class DashboardStack extends NestedStack {
    private final String instanceId;
    private final List<String> tableNames;
    private final String metricsNamespace;
    private final Duration window;
    private final Dashboard dashboard;
    private final IngestStack ingestStack;
    private final CompactionStack compactionStack;
    private final PartitionSplittingStack partitionSplittingStack;

    public DashboardStack(
            Construct scope,
            String id,
            IngestStack ingestStack,
            CompactionStack compactionStack,
            PartitionSplittingStack partitionSplittingStack,
            InstanceProperties instanceProperties,
            List<TableProperties> tableProperties,
            List<IMetric> errorMetrics) {
        super(scope, id);

        this.ingestStack = ingestStack;
        this.compactionStack = compactionStack;
        this.partitionSplittingStack = partitionSplittingStack;

        instanceId = instanceProperties.get(ID);
        tableNames = tableProperties.stream()
                .map(properties -> properties.get(TableProperty.TABLE_NAME))
                .sorted()
                // There's a limit of 500 widgets in a dashboard, including the widgets not associated with a table
                .limit(50)
                .toList();
        metricsNamespace = instanceProperties.get(METRICS_NAMESPACE);
        int timeWindowInMinutes = instanceProperties.getInt(DASHBOARD_TIME_WINDOW_MINUTES);
        window = Duration.minutes(timeWindowInMinutes);
        dashboard = Dashboard.Builder.create(this, "dashboard")
                .dashboardName(Utils.cleanInstanceId(instanceProperties))
                .build();

        addErrorMetricsWidgets(errorMetrics);
        addIngestWidgets();
        addTableWidgets();
        addCompactionWidgets();

        CfnOutput.Builder.create(this, "DashboardUrl")
                .value(constructUrl(instanceProperties))
                .build();

        Utils.addTags(this, instanceProperties);
    }

    private static String constructUrl(InstanceProperties instanceProperties) {
        return "https://" + instanceProperties.get(REGION) + ".console.aws.amazon.com/cloudwatch/home" +
                "#dashboards:name=" + instanceProperties.get(ID) + ";expand=true";
    }

    private void addErrorMetricsWidgets(List<IMetric> errorMetrics) {
        if (!errorMetrics.isEmpty()) {
            dashboard.addWidgets(
                    SingleValueWidget.Builder.create()
                            .title("Errors")
                            .metrics(errorMetrics)
                            .width(24)
                            .build());
        }
    }

    private void addIngestWidgets() {
        if (ingestStack == null) {
            return;
        }

        dashboard.addWidgets(
                TextWidget.Builder.create()
                        .markdown("## Standard Ingest")
                        .width(24)
                        .height(1)
                        .build(),
                GraphWidget.Builder.create()
                        .view(GraphWidgetView.TIME_SERIES)
                        .title("NumberOfJobsSubmitted")
                        .left(List.of(ingestStack.getIngestJobQueue().metricNumberOfMessagesSent(MetricOptions.builder()
                                .unit(Unit.COUNT)
                                .period(window)
                                .statistic("Sum")
                                .build())))
                        .width(6)
                        .build(),
                GraphWidget.Builder.create()
                        .view(GraphWidgetView.TIME_SERIES)
                        .title("NumberOfJobsWaiting")
                        .left(List.of(ingestStack.getIngestJobQueue().metricApproximateNumberOfMessagesVisible(MetricOptions.builder()
                                .unit(Unit.COUNT)
                                .period(window)
                                .statistic("Average")
                                .build())))
                        .width(6)
                        .build(),
                GraphWidget.Builder.create()
                        .view(GraphWidgetView.TIME_SERIES)
                        .title("AgeOfOldestWaitingJob")
                        .left(List.of(ingestStack.getIngestJobQueue().metricApproximateAgeOfOldestMessage(MetricOptions.builder()
                                .unit(Unit.SECONDS)
                                .period(window)
                                .statistic("Maximum")
                                .build())))
                        .width(6)
                        .build());

        if (!tableNames.isEmpty()) {
            dashboard.addWidgets(
                    GraphWidget.Builder.create()
                            .view(GraphWidgetView.TIME_SERIES)
                            .stacked(true)
                            .title("NumRowsWritten")
                            .left(
                                    IntStream.range(0, tableNames.size())
                                            .mapToObj(i -> MathExpression.Builder.create()
                                                    .label(tableNames.get(i))
                                                    .expression("FILL(m" + i + ", 0)")
                                                    .period(window)
                                                    .usingMetrics(Map.of("m" + i, Metric.Builder.create()
                                                            .namespace(metricsNamespace)
                                                            .metricName("StandardIngestRowsWritten")
                                                            .unit(Unit.COUNT)
                                                            .period(window)
                                                            .statistic("Sum")
                                                            .dimensionsMap(createDimensionMap(instanceId, tableNames.get(i)))
                                                            .build()))
                                                    .build())
                                            .toList())
                            .leftYAxis(YAxisProps.builder().min(0).build())
                            .width(6)
                            .build());
        }
    }

    private static Map<String, String> createDimensionMap(String instanceId, String tableName) {
        return Map.of(
                "instanceId", instanceId,
                "tableName", tableName);
    }

    private void addTableWidgets() {
        tableNames.forEach(tableName -> {
            Map<String, String> dimensions = createDimensionMap(instanceId, tableName);

            dashboard.addWidgets(
                    TextWidget.Builder.create()
                            .markdown("## Table: " + tableName)
                            .width(24)
                            .height(1)
                            .build(),
                    GraphWidget.Builder.create()
                            .view(GraphWidgetView.TIME_SERIES)
                            .title("NumberOfFilesWithReferences")
                            .left(List.of(Metric.Builder.create()
                                    .namespace(metricsNamespace)
                                    .metricName("NumberOfFilesWithReferences")
                                    .unit(Unit.COUNT)
                                    .period(window)
                                    .statistic("Average")
                                    .dimensionsMap(dimensions)
                                    .build()))
                            .leftYAxis(YAxisProps.builder().min(0).build())
                            .width(6)
                            .build(),
                    GraphWidget.Builder.create()
                            .view(GraphWidgetView.TIME_SERIES)
                            .title("RowCount")
                            .left(List.of(Metric.Builder.create()
                                    .namespace(metricsNamespace)
                                    .metricName("RowCount")
                                    .unit(Unit.COUNT)
                                    .period(window)
                                    .statistic("Average")
                                    .dimensionsMap(dimensions)
                                    .build()))
                            .leftYAxis(YAxisProps.builder().min(0).build())
                            .width(6)
                            .build(),
                    GraphWidget.Builder.create()
                            .view(GraphWidgetView.TIME_SERIES)
                            .title("Partitions")
                            .left(List.of(
                                    Metric.Builder.create()
                                            .namespace(metricsNamespace)
                                            .metricName("PartitionCount")
                                            .unit(Unit.COUNT)
                                            .period(window)
                                            .statistic("Average")
                                            .dimensionsMap(dimensions)
                                            .build(),
                                    Metric.Builder.create()
                                            .namespace(metricsNamespace)
                                            .metricName("LeafPartitionCount")
                                            .unit(Unit.COUNT)
                                            .period(window)
                                            .statistic("Average")
                                            .dimensionsMap(dimensions)
                                            .build()))
                            .leftYAxis(YAxisProps.builder().min(0).build())
                            .width(6)
                            .build(),
                    GraphWidget.Builder.create()
                            .view(GraphWidgetView.TIME_SERIES)
                            .title("FilesReferencesPerPartition")
                            .left(List.of(Metric.Builder.create()
                                    .namespace(metricsNamespace)
                                    .metricName("AverageFileReferencesPerPartition")
                                    .unit(Unit.COUNT)
                                    .period(window)
                                    .statistic("Average")
                                    .dimensionsMap(dimensions)
                                    .build()))
                            .leftYAxis(YAxisProps.builder().min(0).build())
                            .width(6)
                            .build());
        });
    }

    private void addCompactionWidgets() {
        if (compactionStack == null && partitionSplittingStack == null) {
            return;
        }
        List<Metric> jobsSubmittedMetrics = new ArrayList<>();
        List<Metric> jobsWaitingMetrics = new ArrayList<>();
        List<Metric> oldestJobMetrics = new ArrayList<>();

        if (null != compactionStack) {
            jobsSubmittedMetrics.add(
                    compactionStack.getCompactionJobsQueue().metricNumberOfMessagesSent(MetricOptions.builder()
                            .label("Compaction")
                            .unit(Unit.COUNT)
                            .period(window)
                            .statistic("Sum")
                            .build()));
            jobsWaitingMetrics.add(
                    compactionStack.getCompactionJobsQueue().metricApproximateNumberOfMessagesVisible(MetricOptions.builder()
                            .label("Compaction")
                            .unit(Unit.COUNT)
                            .period(window)
                            .statistic("Average")
                            .build()));
            oldestJobMetrics.add(
                    compactionStack.getCompactionJobsQueue().metricApproximateAgeOfOldestMessage(MetricOptions.builder()
                            .label("Compaction")
                            .unit(Unit.COUNT)
                            .period(window)
                            .statistic("Maximum")
                            .build()));
        }

        if (null != partitionSplittingStack) {
            jobsSubmittedMetrics.add(
                    partitionSplittingStack.getJobQueue().metricNumberOfMessagesSent(MetricOptions.builder()
                            .label("Partition Splits")
                            .unit(Unit.COUNT)
                            .period(window)
                            .statistic("Sum")
                            .build()));
            jobsWaitingMetrics.add(
                    partitionSplittingStack.getJobQueue().metricApproximateNumberOfMessagesVisible(MetricOptions.builder()
                            .label("Partition Splits")
                            .unit(Unit.COUNT)
                            .period(window)
                            .statistic("Average")
                            .build()));
            oldestJobMetrics.add(
                    partitionSplittingStack.getJobQueue().metricApproximateAgeOfOldestMessage(MetricOptions.builder()
                            .label("Partition Splits")
                            .unit(Unit.COUNT)
                            .period(window)
                            .statistic("Maximum")
                            .build()));
        }

        dashboard.addWidgets(
                TextWidget.Builder.create()
                        .markdown("## Compactions and Splits")
                        .width(24)
                        .height(1)
                        .build(),
                GraphWidget.Builder.create()
                        .view(GraphWidgetView.TIME_SERIES)
                        .title("NumberOfJobsSubmitted")
                        .left(jobsSubmittedMetrics)
                        .width(6)
                        .build(),
                GraphWidget.Builder.create()
                        .view(GraphWidgetView.TIME_SERIES)
                        .title("NumberOfJobsWaiting")
                        .left(jobsWaitingMetrics)
                        .width(6)
                        .build(),
                GraphWidget.Builder.create()
                        .view(GraphWidgetView.TIME_SERIES)
                        .title("AgeOfOldestWaitingJob")
                        .left(oldestJobMetrics)
                        .width(6)
                        .build());
    }
}
