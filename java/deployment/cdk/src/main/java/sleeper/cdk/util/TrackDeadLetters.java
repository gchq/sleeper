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
package sleeper.cdk.util;

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.services.cloudwatch.Alarm;
import software.amazon.awscdk.services.cloudwatch.ComparisonOperator;
import software.amazon.awscdk.services.cloudwatch.IMetric;
import software.amazon.awscdk.services.cloudwatch.MetricOptions;
import software.amazon.awscdk.services.cloudwatch.TreatMissingData;
import software.amazon.awscdk.services.cloudwatch.actions.SnsAction;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.stack.core.TopicStack;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.ArrayList;
import java.util.List;

import static sleeper.core.properties.instance.MetricsProperty.DASHBOARD_TIME_WINDOW_MINUTES;

public class TrackDeadLetters {

    private final InstanceProperties instanceProperties;
    private final TopicStack topicStack;
    private final List<IMetric> errorMetrics = new ArrayList<>();

    public TrackDeadLetters(InstanceProperties instanceProperties, TopicStack topicStack) {
        this.instanceProperties = instanceProperties;
        this.topicStack = topicStack;
    }

    public void alarmOnDeadLetters(Construct scope, String id, String description, Queue dlq) {
        createAlarmForDlq(scope, id, "Alarms if there are any messages on the dead letter queue for " + description, dlq);
        errorMetrics.add(createErrorMetric("Dead letters for " + description, dlq));
    }

    public Topic getTopic() {
        return topicStack.getTopic();
    }

    public List<IMetric> getErrorMetrics() {
        return errorMetrics;
    }

    private void createAlarmForDlq(Construct scope, String id, String description, Queue dlq) {
        Alarm alarm = Alarm.Builder
                .create(scope, id)
                .alarmName(dlq.getQueueName())
                .alarmDescription(description)
                .metric(dlq.metricApproximateNumberOfMessagesVisible()
                        .with(MetricOptions.builder().statistic("Sum").period(Duration.seconds(60)).build()))
                .comparisonOperator(ComparisonOperator.GREATER_THAN_THRESHOLD)
                .threshold(0)
                .evaluationPeriods(1)
                .datapointsToAlarm(1)
                .treatMissingData(TreatMissingData.IGNORE)
                .build();
        alarm.addAlarmAction(new SnsAction(topicStack.getTopic()));
    }

    private IMetric createErrorMetric(String label, Queue errorQueue) {
        int timeWindowInMinutes = instanceProperties.getInt(DASHBOARD_TIME_WINDOW_MINUTES);
        return errorQueue.metricApproximateNumberOfMessagesVisible(
                MetricOptions.builder().label(label).period(Duration.minutes(timeWindowInMinutes)).statistic("Sum").build());
    }

}
