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

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.cloudwatch.IMetric;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.cdk.stack.core.CoreStacks;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.deploy.SleeperScheduleRule;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.Collections;
import java.util.List;

import static sleeper.cdk.util.Utils.createAlarmForDlq;
import static sleeper.cdk.util.Utils.shouldDeployPaused;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_DLQ_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_LAMBDA_FUNCTION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_RULE;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.METRICS_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.CommonProperty.METRICS_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.CommonProperty.METRICS_TABLE_BATCH_SIZE;
import static sleeper.core.properties.instance.CommonProperty.TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB;
import static sleeper.core.properties.instance.CommonProperty.TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS;

public class TableMetricsStack extends NestedStack {
    public TableMetricsStack(
            Construct scope, String id, InstanceProperties instanceProperties,
            BuiltJars jars, Topic topic, CoreStacks coreStacks, List<IMetric> errorMetrics) {
        super(scope, id);
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", instanceProperties.get(JARS_BUCKET));
        LambdaCode lambdaCode = jars.lambdaCode(jarsBucket);
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String triggerFunctionName = String.join("-", "sleeper", instanceId, "metrics-trigger");
        String publishFunctionName = String.join("-", "sleeper", instanceId, "metrics-publisher");
        // Metrics generation and publishing
        IFunction tableMetricsTrigger = lambdaCode.buildFunction(this, LambdaHandler.METRICS_TRIGGER, "MetricsTrigger", builder -> builder
                .functionName(triggerFunctionName)
                .description("Creates batches of Sleeper tables to calculate metrics for and puts them on a queue to be published")
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(1)
                .memorySize(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS)))
                .logGroup(coreStacks.getLogGroupByFunctionName(triggerFunctionName)));
        IFunction tableMetricsPublisher = lambdaCode.buildFunction(this, LambdaHandler.METRICS, "MetricsPublisher", builder -> builder
                .functionName(publishFunctionName)
                .description("Generates metrics for a Sleeper table based on info in its state store, and publishes them to CloudWatch")
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(instanceProperties.getInt(METRICS_LAMBDA_CONCURRENCY_RESERVED))
                .memorySize(1024)
                .timeout(Duration.minutes(1))
                .logGroup(coreStacks.getLogGroupByFunctionName(publishFunctionName)));

        instanceProperties.set(TABLE_METRICS_LAMBDA_FUNCTION, tableMetricsTrigger.getFunctionName());

        Rule rule = Rule.Builder.create(this, "MetricsPublishSchedule")
                .ruleName(SleeperScheduleRule.TABLE_METRICS.buildRuleName(instanceProperties))
                .schedule(Schedule.rate(Duration.minutes(1)))
                .targets(Collections.singletonList(new LambdaFunction(tableMetricsTrigger)))
                .enabled(!shouldDeployPaused(this))
                .build();
        instanceProperties.set(TABLE_METRICS_RULE, rule.getRuleName());

        Queue deadLetterQueue = Queue.Builder
                .create(this, "MetricsJobDeadLetterQueue")
                .queueName(String.join("-", "sleeper", instanceId, "MetricsJobDLQ.fifo"))
                .fifo(true)
                .build();
        Queue queue = Queue.Builder
                .create(this, "MetricsJobQueue")
                .queueName(String.join("-", "sleeper", instanceId, "MetricsJobQ.fifo"))
                .deadLetterQueue(DeadLetterQueue.builder()
                        .maxReceiveCount(1)
                        .queue(deadLetterQueue)
                        .build())
                .fifo(true)
                .visibilityTimeout(Duration.seconds(70))
                .build();
        instanceProperties.set(TABLE_METRICS_QUEUE_URL, queue.getQueueUrl());
        instanceProperties.set(TABLE_METRICS_QUEUE_ARN, queue.getQueueArn());
        instanceProperties.set(TABLE_METRICS_DLQ_URL, deadLetterQueue.getQueueUrl());
        instanceProperties.set(TABLE_METRICS_DLQ_ARN, deadLetterQueue.getQueueArn());
        createAlarmForDlq(this, "MetricsJobAlarm",
                "Alarms if there are any messages on the dead letter queue for the table metrics queue",
                deadLetterQueue, topic);
        errorMetrics.add(Utils.createErrorMetric("Table Metrics Errors", deadLetterQueue, instanceProperties));
        tableMetricsPublisher.addEventSource(SqsEventSource.Builder.create(queue)
                .batchSize(instanceProperties.getInt(METRICS_TABLE_BATCH_SIZE)).maxConcurrency(instanceProperties.getInt(METRICS_LAMBDA_CONCURRENCY_MAXIMUM)).build());

        coreStacks.grantReadTablesStatus(tableMetricsTrigger);
        coreStacks.grantReadTablesMetadata(tableMetricsPublisher);
        queue.grantSendMessages(tableMetricsTrigger);
        coreStacks.grantInvokeScheduled(tableMetricsTrigger, queue);
        coreStacks.getReportingPolicyForGrants().addStatements(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("cloudwatch:GetMetricData"))
                .resources(List.of("*"))
                .build());

        Utils.addStackTagIfSet(this, instanceProperties);
    }
}
