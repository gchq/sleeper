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
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSourceProps;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.TracingUtils;
import sleeper.cdk.Utils;
import sleeper.cdk.jars.BuiltJar;
import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.configuration.properties.SleeperScheduleRule;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.Collections;
import java.util.Locale;

import static sleeper.cdk.Utils.createLambdaLogGroup;
import static sleeper.cdk.Utils.shouldDeployPaused;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_DLQ_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_DLQ_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_QUEUE_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_RULE;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.CommonProperty.TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS;

public class TableMetricsStack extends NestedStack {
    public TableMetricsStack(
            Construct scope, String id, InstanceProperties instanceProperties,
            BuiltJars jars, CoreStacks coreStacks) {
        super(scope, id);
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", instanceProperties.get(JARS_BUCKET));
        LambdaCode metricsJar = jars.lambdaCode(BuiltJar.METRICS, jarsBucket);
        String triggerFunctionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "metrics-trigger"));
        String publishFunctionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "metrics-publisher"));
        // Metrics generation and publishing
        IFunction tableMetricsTrigger = metricsJar.buildFunction(this, "MetricsTrigger", builder -> builder
                .functionName(triggerFunctionName)
                .description("Creates batches of Sleeper tables to calculate metrics for and puts them on a queue to be published")
                .runtime(Runtime.JAVA_11)
                .handler("sleeper.metrics.TableMetricsTriggerLambda::handleRequest")
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .memorySize(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS)))
                .logGroup(createLambdaLogGroup(this, "MetricsTriggerLogGroup", triggerFunctionName, instanceProperties))
                .tracing(TracingUtils.active(instanceProperties)));
        IFunction tableMetricsPublisher = metricsJar.buildFunction(this, "MetricsPublisher", builder -> builder
                .functionName(publishFunctionName)
                .description("Generates metrics for a Sleeper table based on info in its state store, and publishes them to CloudWatch")
                .runtime(Runtime.JAVA_11)
                .handler("sleeper.metrics.TableMetricsLambda::handleRequest")
                .memorySize(1024)
                .timeout(Duration.minutes(1))
                .logGroup(createLambdaLogGroup(this, "MetricsPublisherLogGroup", publishFunctionName, instanceProperties))
                .tracing(TracingUtils.passThrough(instanceProperties)));

        coreStacks.grantReadTablesStatus(tableMetricsTrigger);
        coreStacks.grantReadTablesMetadata(tableMetricsPublisher);
        instanceProperties.set(TABLE_METRICS_LAMBDA_FUNCTION, tableMetricsTrigger.getFunctionName());

        Rule rule = Rule.Builder.create(this, "MetricsPublishSchedule")
                .ruleName(SleeperScheduleRule.TABLE_METRICS.buildRuleName(instanceProperties))
                .schedule(Schedule.rate(Duration.minutes(1)))
                .targets(Collections.singletonList(new LambdaFunction(tableMetricsTrigger)))
                .enabled(!shouldDeployPaused(this))
                .build();
        instanceProperties.set(TABLE_METRICS_RULE, rule.getRuleName());

        String deadLetterQueueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-MetricsJobDLQ");
        Queue deadLetterQueue = Queue.Builder
                .create(this, "MetricsJobDeadLetterQueue")
                .queueName(deadLetterQueueName)
                .build();
        String queueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-MetricsJobQ");
        Queue queue = Queue.Builder
                .create(this, "MetricsJobQueue")
                .queueName(queueName)
                .deadLetterQueue(DeadLetterQueue.builder()
                        .maxReceiveCount(1)
                        .queue(deadLetterQueue)
                        .build())
                .visibilityTimeout(Duration.seconds(70))
                .build();
        instanceProperties.set(TABLE_METRICS_QUEUE_URL, queue.getQueueUrl());
        instanceProperties.set(TABLE_METRICS_QUEUE_ARN, queue.getQueueArn());
        instanceProperties.set(TABLE_METRICS_DLQ_URL, deadLetterQueue.getQueueUrl());
        instanceProperties.set(TABLE_METRICS_DLQ_ARN, deadLetterQueue.getQueueArn());

        queue.grantSendMessages(tableMetricsTrigger);
        tableMetricsPublisher.addEventSource(new SqsEventSource(queue,
                SqsEventSourceProps.builder().batchSize(1).build()));

        Utils.addStackTagIfSet(this, instanceProperties);
    }
}
