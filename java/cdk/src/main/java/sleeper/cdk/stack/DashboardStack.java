/*
 * Copyright 2022 Crown Copyright
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import sleeper.cdk.Utils;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DASHBOARD_TIME_WINDOW_MINUTES;
import sleeper.configuration.properties.table.TableProperty;
import software.amazon.awscdk.CfnOutput;
import software.constructs.Construct;
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

public class DashboardStack extends NestedStack {
    private final InstanceProperties instanceProperties;

    public DashboardStack(Construct scope,
            String id,
            IngestStack ingestStack,
            CompactionStack compactionStack,
            PartitionSplittingStack partitionSplittingStack,
            InstanceProperties instanceProperties) {
        super(scope, id);

        this.instanceProperties = instanceProperties;

        String instanceId = this.instanceProperties.get(UserDefinedInstanceProperty.ID);
        String metricsNamespace = this.instanceProperties.get(UserDefinedInstanceProperty.METRICS_NAMESPACE);
        List<String> tableNames = Utils.getAllTableProperties(this.instanceProperties)
                .map(tableProperties -> tableProperties.get(TableProperty.TABLE_NAME))
                .sorted()
                .collect(Collectors.toList());
        int timeWindowInMinutes = this.instanceProperties.getInt(DASHBOARD_TIME_WINDOW_MINUTES);
        Duration window = Duration.minutes(timeWindowInMinutes);

        Dashboard dashboard = Dashboard.Builder.create(this, "dashboard").dashboardName(instanceId).build();

        List<IMetric> errorMetrics = new ArrayList<>();
        if (null != ingestStack) {
            errorMetrics.add(ingestStack.getErrorQueue().metricApproximateNumberOfMessagesVisible(
                    MetricOptions.builder().label("Ingest Errors").period(window).statistic("Sum").build()));
        }
        if (null != compactionStack) {
            errorMetrics.add(compactionStack.getCompactionDeadLetterQueue().metricApproximateNumberOfMessagesVisible(
                    MetricOptions.builder().label("Merge Compaction Errors").period(window).statistic("Sum").build()));
            errorMetrics.add(compactionStack.getSplittingDeadLetterQueue().metricApproximateNumberOfMessagesVisible(
                    MetricOptions.builder().label("Split Compaction Errors").period(window).statistic("Sum").build()));
        }
        if (null != partitionSplittingStack) {
            errorMetrics.add(partitionSplittingStack.getDeadLetterQueue().metricApproximateNumberOfMessagesVisible(
                    MetricOptions.builder().label("Partition Split Errors").period(window).statistic("Sum").build()));
        }

        if (!errorMetrics.isEmpty()) {
            dashboard.addWidgets(
                SingleValueWidget.Builder.create()
                        .title("Errors")
                        .metrics(errorMetrics)
                        .width(24)
                        .build()
            );
        }

        if (null != ingestStack) {
            dashboard.addWidgets(
                TextWidget.Builder.create()
                        .markdown("## Standard Ingest")
                        .width(24)
                        .height(1)
                        .build(),
                GraphWidget.Builder.create()
                        .view(GraphWidgetView.TIME_SERIES)
                        .title("NumberOfJobsSubmitted")
                        .left(Collections.singletonList(ingestStack.getIngestJobQueue().metricNumberOfMessagesSent(MetricOptions.builder()
                                .unit(Unit.COUNT)
                                .period(window)
                                .statistic("Sum")
                                .build())))
                        .width(6)
                        .build(),
                GraphWidget.Builder.create()
                        .view(GraphWidgetView.TIME_SERIES)
                        .title("NumberOfJobsWaiting")
                        .left(Collections.singletonList(ingestStack.getIngestJobQueue().metricApproximateNumberOfMessagesVisible(MetricOptions.builder()
                                .unit(Unit.COUNT)
                                .period(window)
                                .statistic("Average")
                                .build())))
                        .width(6)
                        .build(),
                GraphWidget.Builder.create()
                        .view(GraphWidgetView.TIME_SERIES)
                        .title("AgeOfOldestWaitingJob")
                        .left(Collections.singletonList(ingestStack.getIngestJobQueue().metricApproximateAgeOfOldestMessage(MetricOptions.builder()
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
                                                .dimensionsMap(new HashMap<String, String>() {
                                                    {
                                                        put("instanceId", instanceId);
                                                        put("tableName", tableNames.get(i));
                                                    }
                                                })
                                                .build()))
                                        .build())
                                        .collect(Collectors.toList())
                        )
                        .leftYAxis(YAxisProps.builder().min(0).build())
                        .width(6)
                        .build()
            );
        }

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
                                        .build()
                        ))
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
                        .build()
            );
        });

        if (null != compactionStack || null != partitionSplittingStack) {
            List<Metric> jobsSubmittedMetrics = new ArrayList<>();
            List<Metric> jobsWaitingMetrics = new ArrayList<>();
            List<Metric> oldestJobMetrics = new ArrayList<>();

            if (null != compactionStack) {
                jobsSubmittedMetrics.add(
                    compactionStack.getCompactionJobsQueue().metricNumberOfMessagesSent(MetricOptions.builder()
                            .label("Merge Compaction")
                            .unit(Unit.COUNT)
                            .period(window)
                            .statistic("Sum")
                            .build())
                );
                jobsSubmittedMetrics.add(
                    compactionStack.getSplittingJobsQueue().metricNumberOfMessagesSent(MetricOptions.builder()
                            .label("Split Compaction")
                            .unit(Unit.COUNT)
                            .period(window)
                            .statistic("Sum")
                            .build())
                );
                jobsWaitingMetrics.add(
                    compactionStack.getCompactionJobsQueue().metricApproximateNumberOfMessagesVisible(MetricOptions.builder()
                            .label("Merge Compaction")
                            .unit(Unit.COUNT)
                            .period(window)
                            .statistic("Average")
                            .build())
                );
                jobsWaitingMetrics.add(
                    compactionStack.getSplittingJobsQueue().metricApproximateNumberOfMessagesVisible(MetricOptions.builder()
                            .label("Split Compaction")
                            .unit(Unit.COUNT)
                            .period(window)
                            .statistic("Average")
                            .build())
                );
                oldestJobMetrics.add(
                    compactionStack.getCompactionJobsQueue().metricApproximateAgeOfOldestMessage(MetricOptions.builder()
                            .label("Merge Compaction")
                            .unit(Unit.COUNT)
                            .period(window)
                            .statistic("Maximum")
                            .build())
                );
                oldestJobMetrics.add(
                    compactionStack.getSplittingJobsQueue().metricApproximateAgeOfOldestMessage(MetricOptions.builder()
                            .label("Split Compaction")
                            .unit(Unit.COUNT)
                            .period(window)
                            .statistic("Maximum")
                            .build())
                );
            }

            if (null != partitionSplittingStack) {
                jobsSubmittedMetrics.add(
                    partitionSplittingStack.getJobQueue().metricNumberOfMessagesSent(MetricOptions.builder()
                            .label("Partition Splits")
                            .unit(Unit.COUNT)
                            .period(window)
                            .statistic("Sum")
                            .build())
                );
                jobsWaitingMetrics.add(
                    partitionSplittingStack.getJobQueue().metricApproximateNumberOfMessagesVisible(MetricOptions.builder()
                            .label("Partition Splits")
                            .unit(Unit.COUNT)
                            .period(window)
                            .statistic("Average")
                            .build())
                );
                oldestJobMetrics.add(
                    partitionSplittingStack.getJobQueue().metricApproximateAgeOfOldestMessage(MetricOptions.builder()
                            .label("Partition Splits")
                            .unit(Unit.COUNT)
                            .period(window)
                            .statistic("Maximum")
                            .build())
                );
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
                        .build()
            );
        }

        CfnOutput.Builder.create(this, "DashboardUrl")
                .value("https://" + this.getRegion() + ".console.aws.amazon.com/cloudwatch/home#dashboards:name=" + instanceId + ";expand=true")
                .build();
    }
}
