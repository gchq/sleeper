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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.SleeperInstanceProps;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.deploy.SleeperScheduleRule;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.GARBAGE_COLLECTOR_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.GARBAGE_COLLECTOR_DLQ_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.GARBAGE_COLLECTOR_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.GARBAGE_COLLECTOR_LAMBDA_FUNCTION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.GARBAGE_COLLECTOR_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.GARBAGE_COLLECTOR_QUEUE_URL;
import static sleeper.core.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_PERIOD_IN_MINUTES;
import static sleeper.core.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_TABLE_BATCH_SIZE;
import static sleeper.core.properties.instance.TableStateProperty.TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB;
import static sleeper.core.properties.instance.TableStateProperty.TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS;

/**
 * Deploys resources to perform garbage collection. This will find and delete files which have been marked as being
 * ready for garbage collection after a compaction job.
 */
@SuppressFBWarnings("MC_OVERRIDABLE_METHOD_CALL_IN_CONSTRUCTOR")
public class GarbageCollectorStack extends NestedStack {

    public GarbageCollectorStack(
            Construct scope, String id, SleeperInstanceProps props, SleeperCoreStacks coreStacks) {
        super(scope, id);
        InstanceProperties instanceProperties = props.getInstanceProperties();
        SleeperLambdaCode lambdaCode = props.getArtefacts().lambdaCodeAtScope(this);

        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String triggerFunctionName = String.join("-", "sleeper", instanceId, "garbage-collector-trigger");
        String functionName = String.join("-", "sleeper", instanceId, "garbage-collector");

        Duration handlerTimeout = Duration.seconds(instanceProperties.getInt(GARBAGE_COLLECTOR_LAMBDA_TIMEOUT_IN_SECONDS));

        // Garbage collector function
        IFunction triggerFunction = lambdaCode.buildFunction(this, LambdaHandler.GARBAGE_COLLECTOR_TRIGGER, "GarbageCollectorTrigger", builder -> builder
                .functionName(triggerFunctionName)
                .description("Creates batches of Sleeper tables to perform garbage collection for and puts them on a queue to be processed")
                .environment(EnvironmentUtils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(1)
                .memorySize(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS)))
                .logGroup(coreStacks.getLogGroup(LogGroupRef.GARBAGE_COLLECTOR_TRIGGER)));
        IFunction handlerFunction = lambdaCode.buildFunction(this, LambdaHandler.GARBAGE_COLLECTOR, "GarbageCollectorLambda", builder -> builder
                .functionName(functionName)
                .description("Scan the state store looking for files that need deleting and delete them")
                .memorySize(instanceProperties.getInt(GARBAGE_COLLECTOR_LAMBDA_MEMORY_IN_MB))
                .timeout(handlerTimeout)
                .environment(EnvironmentUtils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(instanceProperties.getIntOrNull(GARBAGE_COLLECTOR_LAMBDA_CONCURRENCY_RESERVED))
                .logGroup(coreStacks.getLogGroup(LogGroupRef.GARBAGE_COLLECTOR)));
        instanceProperties.set(GARBAGE_COLLECTOR_LAMBDA_FUNCTION, triggerFunction.getFunctionName());

        // Grant this function permission delete files from the data bucket and
        // to read from / write to the DynamoDB table
        coreStacks.grantGarbageCollection(handlerFunction);
        coreStacks.grantReadTablesStatus(triggerFunction);

        // Cloudwatch rule to trigger this lambda
        Rule rule = Rule.Builder
                .create(this, "GarbageCollectorPeriodicTrigger")
                .ruleName(SleeperScheduleRule.GARBAGE_COLLECTOR.buildRuleName(instanceProperties))
                .description(SleeperScheduleRule.GARBAGE_COLLECTOR.getDescription())
                .enabled(!props.isDeployPaused())
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(GARBAGE_COLLECTOR_PERIOD_IN_MINUTES))))
                .targets(List.of(new LambdaFunction(triggerFunction)))
                .build();
        instanceProperties.set(GARBAGE_COLLECTOR_CLOUDWATCH_RULE, rule.getRuleName());

        Queue deadLetterQueue = Queue.Builder
                .create(this, "GCJobDeadLetterQueue")
                .queueName(String.join("-", "sleeper", instanceId, "GCJobDLQ.fifo"))
                .fifo(true)
                .build();
        Queue queue = Queue.Builder
                .create(this, "GCJobQueue")
                .queueName(String.join("-", "sleeper", instanceId, "GCJobQ.fifo"))
                .deadLetterQueue(DeadLetterQueue.builder()
                        .maxReceiveCount(1)
                        .queue(deadLetterQueue)
                        .build())
                .fifo(true)
                .visibilityTimeout(handlerTimeout)
                .build();
        instanceProperties.set(GARBAGE_COLLECTOR_QUEUE_URL, queue.getQueueUrl());
        instanceProperties.set(GARBAGE_COLLECTOR_QUEUE_ARN, queue.getQueueArn());
        instanceProperties.set(GARBAGE_COLLECTOR_DLQ_URL, deadLetterQueue.getQueueUrl());
        instanceProperties.set(GARBAGE_COLLECTOR_DLQ_ARN, deadLetterQueue.getQueueArn());
        coreStacks.alarmOnDeadLetters(this, "GarbageCollectorAlarm", "garbage collection", deadLetterQueue);
        queue.grantSendMessages(triggerFunction);
        handlerFunction.addEventSource(SqsEventSource.Builder.create(queue)
                .batchSize(instanceProperties.getInt(GARBAGE_COLLECTOR_TABLE_BATCH_SIZE))
                .maxConcurrency(instanceProperties.getIntOrNull(GARBAGE_COLLECTOR_LAMBDA_CONCURRENCY_MAXIMUM))
                .build());
        coreStacks.grantInvokeScheduled(triggerFunction, queue);

        Utils.addTags(this, instanceProperties);
    }
}
