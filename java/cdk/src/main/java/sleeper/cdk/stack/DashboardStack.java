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
package sleeper.cdk.stack;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.METRICS_NAMESPACE;
import static sleeper.configuration.properties.instance.DashboardProperty.DASHBOARD_TIME_WINDOW_MINUTES;

@SuppressFBWarnings("MC_OVERRIDABLE_METHOD_CALL_IN_CONSTRUCTOR")
public class DashboardStack extends NestedStack {
    private final String instanceId;
    private final List<String> tableNames;
    private final String metricsNamespace;
    private final Duration window;
    private final Dashboard dashboard;
    private final List<IMetric> errorMetrics = new ArrayList<>();
    private final List<Metric> compactionAndSplittingJobsSubmitted = new ArrayList<>();
    private final List<Metric> compactionAndSplittingJobsWaiting = new ArrayList<>();
    private final List<Metric> oldestCompactionAndSplittingJobs = new ArrayList<>();

    public DashboardStack(Construct scope, String id, InstanceProperties instanceProperties) {
        super(scope, id);

        instanceId = instanceProperties.get(ID);
        tableNames = Utils.getAllTableProperties(instanceProperties, this)
                .map(tableProperties -> tableProperties.get(TableProperty.TABLE_NAME))
                .sorted()
                // There's a limit of 500 widgets in a dashboard, including the widgets not associated with a table
                .limit(50)
                .collect(Collectors.toList());
        metricsNamespace = instanceProperties.get(METRICS_NAMESPACE);
        int timeWindowInMinutes = instanceProperties.getInt(DASHBOARD_TIME_WINDOW_MINUTES);
        window = Duration.minutes(timeWindowInMinutes);
        dashboard = Dashboard.Builder.create(this, "dashboard").dashboardName(instanceId).build();

        addTableWidgets();

        CfnOutput.Builder.create(this, "DashboardUrl")
                .value(constructUrl())
                .build();

        Utils.addStackTagIfSet(this, instanceProperties);
    }

    private String constructUrl() {
        return "https://" + this.getRegion() + ".console.aws.amazon.com/cloudwatch/home#dashboards:name=" + instanceId + ";expand=true";
    }

    public void addErrorMetric(String label, Queue errorQueue) {
        errorMetrics.add(errorQueue.metricApproximateNumberOfMessagesVisible(
                MetricOptions.builder().label(label).period(window).statistic("Sum").build()));
    }

    public void addErrorMetricsWidgets() {
        if (!errorMetrics.isEmpty()) {
            dashboard.addWidgets(
                    SingleValueWidget.Builder.create()
                            .title("Errors")
                            .metrics(errorMetrics)
                            .width(24)
                            .build());
        }
    }

    public void addIngestWidgets(Queue jobQueue) {
        dashboard.addWidgets(
                TextWidget.Builder.create()
                        .markdown("## Standard Ingest")
                        .width(24)
                        .height(1)
                        .build(),
                GraphWidget.Builder.create()
                        .view(GraphWidgetView.TIME_SERIES)
                        .title("NumberOfJobsSubmitted")
                        .left(Collections.singletonList(jobQueue.metricNumberOfMessagesSent(MetricOptions.builder()
                                .unit(Unit.COUNT)
                                .period(window)
                                .statistic("Sum")
                                .build())))
                        .width(6)
                        .build(),
                GraphWidget.Builder.create()
                        .view(GraphWidgetView.TIME_SERIES)
                        .title("NumberOfJobsWaiting")
                        .left(Collections.singletonList(jobQueue.metricApproximateNumberOfMessagesVisible(MetricOptions.builder()
                                .unit(Unit.COUNT)
                                .period(window)
                                .statistic("Average")
                                .build())))
                        .width(6)
                        .build(),
                GraphWidget.Builder.create()
                        .view(GraphWidgetView.TIME_SERIES)
                        .title("AgeOfOldestWaitingJob")
                        .left(Collections.singletonList(jobQueue.metricApproximateAgeOfOldestMessage(MetricOptions.builder()
                                .unit(Unit.SECONDS)
                                .period(window)
                                .statistic("Maximum")
                                .build())))
                        .width(6)
                        .build(),
                GraphWidget.Builder.create()
                        .view(GraphWidgetView.TIME_SERIES)
                        .stacked(true)
                        .title("NumRecordsWritten")
                        .left(
                                IntStream.range(0, tableNames.size())
                                        .mapToObj(i -> MathExpression.Builder.create()
                                                .label(tableNames.get(i))
                                                .expression("FILL(m" + i + ", 0)")
                                                .period(window)
                                                .usingMetrics(Collections.singletonMap("m" + i, Metric.Builder.create()
                                                        .namespace(metricsNamespace)
                                                        .metricName("StandardIngestRecordsWritten")
                                                        .unit(Unit.COUNT)
                                                        .period(window)
                                                        .statistic("Sum")
                                                        .dimensionsMap(createDimensionMap(instanceId, tableNames.get(i)))
                                                        .build()))
                                                .build())
                                        .collect(Collectors.toList()))
                        .leftYAxis(YAxisProps.builder().min(0).build())
                        .width(6)
                        .build());
    }

    private static Map<String, String> createDimensionMap(String instanceId, String tableName) {
        Map<String, String> hashMap = new HashMap<>();
        hashMap.put("instanceId", instanceId);
        hashMap.put("tableName", tableName);
        return hashMap;
    }

    private void addTableWidgets() {
        tableNames.forEach(tableName -> {
            Map<String, String> dimensions = new HashMap<>();
            dimensions.put("instanceId", instanceId);
            dimensions.put("tableName", tableName);

            dashboard.addWidgets(
                    TextWidget.Builder.create()
                            .markdown("## Table: " + tableName)
                            .width(24)
                            .height(1)
                            .build(),
                    GraphWidget.Builder.create()
                            .view(GraphWidgetView.TIME_SERIES)
                            .title("ActiveFileCount")
                            .left(Collections.singletonList(Metric.Builder.create()
                                    .namespace(metricsNamespace)
                                    .metricName("ActiveFileCount")
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
                            .title("RecordCount")
                            .left(Collections.singletonList(Metric.Builder.create()
                                    .namespace(metricsNamespace)
                                    .metricName("RecordCount")
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
                            .left(Arrays.asList(
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
                            .title("FilesPerPartition")
                            .left(Collections.singletonList(Metric.Builder.create()
                                    .namespace(metricsNamespace)
                                    .metricName("AverageActiveFilesPerPartition")
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

    public void addCompactionMetrics(Queue jobQueue) {
        addCompactionAndSplittingMetrics("Compaction", jobQueue);
    }

    public void addPartitionSplittingMetrics(Queue jobQueue) {
        addCompactionAndSplittingMetrics("Partition Splits", jobQueue);
    }

    private void addCompactionAndSplittingMetrics(String label, Queue jobQueue) {
        compactionAndSplittingJobsSubmitted.add(
                jobQueue.metricNumberOfMessagesSent(MetricOptions.builder()
                        .label(label)
                        .unit(Unit.COUNT)
                        .period(window)
                        .statistic("Sum")
                        .build()));
        compactionAndSplittingJobsWaiting.add(
                jobQueue.metricApproximateNumberOfMessagesVisible(MetricOptions.builder()
                        .label(label)
                        .unit(Unit.COUNT)
                        .period(window)
                        .statistic("Average")
                        .build()));
        oldestCompactionAndSplittingJobs.add(
                jobQueue.metricApproximateAgeOfOldestMessage(MetricOptions.builder()
                        .label(label)
                        .unit(Unit.COUNT)
                        .period(window)
                        .statistic("Maximum")
                        .build()));
    }

    public void addCompactionWidgets() {
        dashboard.addWidgets(
                TextWidget.Builder.create()
                        .markdown("## Compactions and Splits")
                        .width(24)
                        .height(1)
                        .build(),
                GraphWidget.Builder.create()
                        .view(GraphWidgetView.TIME_SERIES)
                        .title("NumberOfJobsSubmitted")
                        .left(compactionAndSplittingJobsSubmitted)
                        .width(6)
                        .build(),
                GraphWidget.Builder.create()
                        .view(GraphWidgetView.TIME_SERIES)
                        .title("NumberOfJobsWaiting")
                        .left(compactionAndSplittingJobsWaiting)
                        .width(6)
                        .build(),
                GraphWidget.Builder.create()
                        .view(GraphWidgetView.TIME_SERIES)
                        .title("AgeOfOldestWaitingJob")
                        .left(oldestCompactionAndSplittingJobs)
                        .width(6)
                        .build());
    }
}
