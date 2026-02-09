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

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.SleeperInstanceProps;
import sleeper.cdk.jars.SleeperLambdaCode;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.deploy.SleeperScheduleRule;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_DLQ_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_LAMBDA_FUNCTION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_RULE;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.MetricsProperty.METRICS_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.MetricsProperty.METRICS_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.MetricsProperty.METRICS_TABLE_BATCH_SIZE;
import static sleeper.core.properties.instance.TableStateProperty.TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB;
import static sleeper.core.properties.instance.TableStateProperty.TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS;

public class TableMetricsStack extends NestedStack {
    public TableMetricsStack(
            Construct scope, String id, SleeperInstanceProps props, SleeperCoreStacks coreStacks) {
        super(scope, id);
        InstanceProperties instanceProperties = props.getInstanceProperties();
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", instanceProperties.get(JARS_BUCKET));
        SleeperLambdaCode lambdaCode = props.getJars().lambdaCode(jarsBucket);
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String triggerFunctionName = String.join("-", "sleeper", instanceId, "metrics-trigger");
        String publishFunctionName = String.join("-", "sleeper", instanceId, "metrics-publisher");
        // Metrics generation and publishing
        IFunction tableMetricsTrigger = lambdaCode.buildFunction(this, LambdaHandler.METRICS_TRIGGER, "MetricsTrigger", builder -> builder
                .functionName(triggerFunctionName)
                .description("Creates batches of Sleeper tables to calculate metrics for and puts them on a queue to be published")
                .environment(EnvironmentUtils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(1)
                .memorySize(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS)))
                .logGroup(coreStacks.getLogGroup(LogGroupRef.METRICS_TRIGGER)));
        IFunction tableMetricsPublisher = lambdaCode.buildFunction(this, LambdaHandler.METRICS, "MetricsPublisher", builder -> builder
                .functionName(publishFunctionName)
                .description("Generates metrics for a Sleeper table based on info in its state store, and publishes them to CloudWatch")
                .environment(EnvironmentUtils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(instanceProperties.getIntOrNull(METRICS_LAMBDA_CONCURRENCY_RESERVED))
                .memorySize(1024)
                .timeout(Duration.minutes(1))
                .logGroup(coreStacks.getLogGroup(LogGroupRef.METRICS_PUBLISHER)));

        instanceProperties.set(TABLE_METRICS_LAMBDA_FUNCTION, tableMetricsTrigger.getFunctionName());

        Rule rule = Rule.Builder.create(this, "MetricsPublishSchedule")
                .ruleName(SleeperScheduleRule.TABLE_METRICS.buildRuleName(instanceProperties))
                .description(SleeperScheduleRule.TABLE_METRICS.getDescription())
                .schedule(Schedule.rate(Duration.minutes(1)))
                .targets(List.of(new LambdaFunction(tableMetricsTrigger)))
                .enabled(!props.isDeployPaused())
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
        coreStacks.alarmOnDeadLetters(this, "MetricsJobAlarm", "table metrics generation triggers", deadLetterQueue);
        tableMetricsPublisher.addEventSource(SqsEventSource.Builder.create(queue)
                .batchSize(instanceProperties.getInt(METRICS_TABLE_BATCH_SIZE))
                .maxConcurrency(instanceProperties.getIntOrNull(METRICS_LAMBDA_CONCURRENCY_MAXIMUM))
                .build());

        coreStacks.grantReadTablesStatus(tableMetricsTrigger);
        coreStacks.grantReadTablesMetadata(tableMetricsPublisher);
        queue.grantSendMessages(tableMetricsTrigger);
        coreStacks.grantInvokeScheduled(tableMetricsTrigger, queue);
        coreStacks.getReportingPolicyForGrants().addStatements(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("cloudwatch:GetMetricData"))
                .resources(List.of("*"))
                .build());

        Utils.addTags(this, instanceProperties);
    }
}
