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
package sleeper.cdk.stack.compaction;

import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.services.cloudwatch.IMetric;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;

import sleeper.cdk.jars.LambdaCode;
import sleeper.cdk.stack.core.CoreStacks;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.deploy.SleeperScheduleRule;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.List;
import java.util.Map;

import static sleeper.cdk.util.Utils.createAlarmForDlq;
import static sleeper.cdk.util.Utils.shouldDeployPaused;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_DLQ_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_TRIGGER_LAMBDA_FUNCTION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_DLQ_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_DLQ_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB;
import static sleeper.core.properties.instance.CommonProperty.TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_BATCH_SIZE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_DISPATCH_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_DISPATCH_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_DISPATCH_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_DISPATCH_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_MAX_RETRIES;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;

public class CompactionJobResources {
    private static final String COMPACTION_STACK_QUEUE_URL = "CompactionStackQueueUrlKey";
    private static final String COMPACTION_STACK_DLQ_URL = "CompactionStackDLQUrlKey";

    private final InstanceProperties instanceProperties;
    private final Stack stack;
    private final Queue compactionJobsQueue;

    public CompactionJobResources(Stack stack,
            InstanceProperties instanceProperties,
            LambdaCode lambdaCode,
            IBucket jarsBucket,
            Topic topic,
            CoreStacks coreStacks,
            List<IMetric> errorMetrics) {
        this.instanceProperties = instanceProperties;
        this.stack = stack;

        Queue pendingQueue = sqsQueueForCompactionJobBatches(coreStacks, topic, errorMetrics);
        compactionJobsQueue = sqsQueueForCompactionJobs(coreStacks, topic, errorMetrics);
        lambdaToCreateCompactionJobBatches(coreStacks, topic, errorMetrics, jarsBucket, lambdaCode, pendingQueue, compactionJobsQueue);
        lambdaToSendCompactionJobBatches(coreStacks, lambdaCode, pendingQueue, compactionJobsQueue);
    }

    private Queue sqsQueueForCompactionJobs(CoreStacks coreStacks, Topic topic, List<IMetric> errorMetrics) {
        // Create queue for compaction job definitions
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String dlQueueName = String.join("-", "sleeper", instanceId, "CompactionJobDLQ");
        Queue compactionDLQ = Queue.Builder
                .create(stack, "CompactionJobDefinitionsDeadLetterQueue")
                .queueName(dlQueueName)
                .build();
        DeadLetterQueue compactionJobDefinitionsDeadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(instanceProperties.getInt(COMPACTION_JOB_MAX_RETRIES))
                .queue(compactionDLQ)
                .build();
        String queueName = String.join("-", "sleeper", instanceId, "CompactionJobQ");
        Queue compactionJobQ = Queue.Builder
                .create(stack, "CompactionJobDefinitionsQueue")
                .queueName(queueName)
                .deadLetterQueue(compactionJobDefinitionsDeadLetterQueue)
                .visibilityTimeout(
                        Duration.seconds(instanceProperties.getInt(COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, compactionJobQ.getQueueUrl());
        instanceProperties.set(COMPACTION_JOB_QUEUE_ARN, compactionJobQ.getQueueArn());
        instanceProperties.set(COMPACTION_JOB_DLQ_URL,
                compactionJobDefinitionsDeadLetterQueue.getQueue().getQueueUrl());
        instanceProperties.set(COMPACTION_JOB_DLQ_ARN,
                compactionJobDefinitionsDeadLetterQueue.getQueue().getQueueArn());

        // Add alarm to send message to SNS if there are any messages on the dead letter queue
        createAlarmForDlq(stack, "CompactionAlarm",
                "Alarms if there are any messages on the dead letter queue for the compactions queue",
                compactionDLQ, topic);
        errorMetrics.add(Utils.createErrorMetric("Compaction Errors", compactionDLQ, instanceProperties));
        compactionJobQ.grantPurge(coreStacks.getPurgeQueuesPolicyForGrants());

        CfnOutputProps compactionJobDefinitionsQueueProps = new CfnOutputProps.Builder()
                .value(compactionJobQ.getQueueUrl())
                .build();
        new CfnOutput(stack, COMPACTION_STACK_QUEUE_URL, compactionJobDefinitionsQueueProps);
        CfnOutputProps compactionJobDefinitionsDLQueueProps = new CfnOutputProps.Builder()
                .value(compactionJobDefinitionsDeadLetterQueue.getQueue().getQueueUrl())
                .build();
        new CfnOutput(stack, COMPACTION_STACK_DLQ_URL, compactionJobDefinitionsDLQueueProps);

        return compactionJobQ;
    }

    private void lambdaToCreateCompactionJobBatches(
            CoreStacks coreStacks, Topic topic, List<IMetric> errorMetrics,
            IBucket jarsBucket, LambdaCode lambdaCode, Queue pendingQueue, Queue compactionJobsQueue) {

        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String triggerFunctionName = String.join("-", "sleeper", instanceId, "compaction-job-creation-trigger");
        String functionName = String.join("-", "sleeper", instanceId, "compaction-job-creation-handler");
        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);

        IFunction triggerFunction = lambdaCode.buildFunction(stack, LambdaHandler.COMPACTION_JOB_CREATOR_TRIGGER, "CompactionJobsCreationTrigger", builder -> builder
                .functionName(triggerFunctionName)
                .description("Create batches of tables and send requests to create compaction jobs for those batches")
                .memorySize(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS)))
                .environment(environmentVariables)
                .reservedConcurrentExecutions(1)
                .logGroup(coreStacks.getLogGroupByFunctionName(triggerFunctionName)));

        IFunction handlerFunction = lambdaCode.buildFunction(stack, LambdaHandler.COMPACTION_JOB_CREATOR, "CompactionJobsCreationHandler", builder -> builder
                .functionName(functionName)
                .description("Scan the state stores of the provided tables looking for compaction jobs to create")
                .memorySize(instanceProperties.getInt(COMPACTION_JOB_CREATION_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS)))
                .environment(environmentVariables)
                .reservedConcurrentExecutions(instanceProperties.getInt(COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_RESERVED))
                .logGroup(coreStacks.getLogGroupByFunctionName(functionName)));

        // Send messages from the trigger function to the handler function
        Queue jobCreationQueue = sqsQueueForCompactionJobCreation(coreStacks, topic, errorMetrics);
        handlerFunction.addEventSource(SqsEventSource.Builder.create(jobCreationQueue)
                .batchSize(instanceProperties.getInt(COMPACTION_JOB_CREATION_BATCH_SIZE))
                .maxConcurrency(instanceProperties.getInt(COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_MAXIMUM))
                .build());

        // Grant permissions
        // - Read through tables in trigger, send batches
        // - Read/write for creating compaction jobs, access to jars bucket for compaction strategies
        jobCreationQueue.grantSendMessages(triggerFunction);
        coreStacks.grantReadTablesStatus(triggerFunction);
        coreStacks.grantCreateCompactionJobs(handlerFunction);
        jarsBucket.grantRead(handlerFunction);
        compactionJobsQueue.grantSendMessages(handlerFunction);
        pendingQueue.grantSendMessages(handlerFunction);
        coreStacks.grantInvokeScheduled(triggerFunction, jobCreationQueue);
        coreStacks.grantCreateCompactionJobs(coreStacks.getInvokeCompactionPolicyForGrants());
        compactionJobsQueue.grantSendMessages(coreStacks.getInvokeCompactionPolicyForGrants());
        pendingQueue.grantSendMessages(coreStacks.getInvokeCompactionPolicyForGrants());

        // Cloudwatch rule to trigger stack lambda
        Rule rule = Rule.Builder
                .create(stack, "CompactionJobCreationPeriodicTrigger")
                .ruleName(SleeperScheduleRule.COMPACTION_JOB_CREATION.buildRuleName(instanceProperties))
                .description("A rule to periodically trigger the compaction job creation lambda")
                .enabled(!shouldDeployPaused(stack))
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(COMPACTION_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES))))
                .targets(List.of(new LambdaFunction(triggerFunction)))
                .build();
        instanceProperties.set(COMPACTION_JOB_CREATION_TRIGGER_LAMBDA_FUNCTION, triggerFunction.getFunctionName());
        instanceProperties.set(COMPACTION_JOB_CREATION_CLOUDWATCH_RULE, rule.getRuleName());
    }

    private void lambdaToSendCompactionJobBatches(
            CoreStacks coreStacks, LambdaCode lambdaCode, Queue pendingQueue, Queue compactionJobsQueue) {

        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String functionName = String.join("-", "sleeper", instanceId, "compaction-job-dispatcher");

        IFunction function = lambdaCode.buildFunction(stack, LambdaHandler.COMPACTION_JOB_DISPATCHER, "CompactionJobDispatcher", builder -> builder
                .functionName(functionName)
                .description("Sends batches of compaction jobs created by the job creation lambda")
                .memorySize(instanceProperties.getInt(COMPACTION_JOB_DISPATCH_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(COMPACTION_JOB_DISPATCH_LAMBDA_TIMEOUT_IN_SECONDS)))
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(instanceProperties.getInt(COMPACTION_JOB_DISPATCH_LAMBDA_CONCURRENCY_RESERVED))
                .logGroup(coreStacks.getLogGroupByFunctionName(functionName)));

        function.addEventSource(SqsEventSource.Builder.create(pendingQueue)
                .batchSize(1)
                .maxConcurrency(instanceProperties.getInt(COMPACTION_JOB_DISPATCH_LAMBDA_CONCURRENCY_MAXIMUM))
                .build());
        coreStacks.grantCreateCompactionJobs(function);
        compactionJobsQueue.grantSendMessages(function);
    }

    private Queue sqsQueueForCompactionJobCreation(CoreStacks coreStacks, Topic topic, List<IMetric> errorMetrics) {
        // Create queue for compaction job creation invocation
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        Queue deadLetterQueue = Queue.Builder
                .create(stack, "CompactionJobCreationDLQ")
                .queueName(String.join("-", "sleeper", instanceId, "CompactionJobCreationDLQ.fifo"))
                .fifo(true)
                .build();
        Queue queue = Queue.Builder
                .create(stack, "CompactionJobCreationQueue")
                .queueName(String.join("-", "sleeper", instanceId, "CompactionJobCreationQ.fifo"))
                .deadLetterQueue(DeadLetterQueue.builder()
                        .maxReceiveCount(1)
                        .queue(deadLetterQueue)
                        .build())
                .fifo(true)
                .visibilityTimeout(
                        Duration.seconds(instanceProperties.getInt(COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(COMPACTION_JOB_CREATION_QUEUE_URL, queue.getQueueUrl());
        instanceProperties.set(COMPACTION_JOB_CREATION_QUEUE_ARN, queue.getQueueArn());
        instanceProperties.set(COMPACTION_JOB_CREATION_DLQ_URL, deadLetterQueue.getQueueUrl());
        instanceProperties.set(COMPACTION_JOB_CREATION_DLQ_ARN, deadLetterQueue.getQueueArn());

        createAlarmForDlq(stack, "CompactionJobCreationBatchAlarm",
                "Alarms if there are any messages on the dead letter queue for compaction job creation",
                deadLetterQueue, topic);
        errorMetrics.add(Utils.createErrorMetric("Compaction Batching Errors", deadLetterQueue, instanceProperties));
        queue.grantPurge(coreStacks.getPurgeQueuesPolicyForGrants());
        return queue;
    }

    private Queue sqsQueueForCompactionJobBatches(CoreStacks coreStacks, Topic topic, List<IMetric> errorMetrics) {
        // Create queue for compaction job definitions
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String dlQueueName = String.join("-", "sleeper", instanceId, "PendingCompactionJobBatchDLQ");
        Queue pendingDLQ = Queue.Builder
                .create(stack, "PendingCompactionJobBatchDLQ")
                .queueName(dlQueueName)
                .build();
        String queueName = String.join("-", "sleeper", instanceId, "PendingCompactionJobBatchQ");
        Queue pendingQ = Queue.Builder
                .create(stack, "PendingCompactionJobBatchQ")
                .queueName(queueName)
                .deadLetterQueue(DeadLetterQueue.builder()
                        .maxReceiveCount(instanceProperties.getInt(COMPACTION_JOB_MAX_RETRIES))
                        .queue(pendingDLQ)
                        .build())
                .visibilityTimeout(
                        Duration.seconds(instanceProperties.getInt(COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(COMPACTION_PENDING_QUEUE_URL, pendingQ.getQueueUrl());
        instanceProperties.set(COMPACTION_PENDING_QUEUE_ARN, pendingQ.getQueueArn());
        instanceProperties.set(COMPACTION_PENDING_DLQ_URL, pendingDLQ.getQueueUrl());
        instanceProperties.set(COMPACTION_PENDING_DLQ_ARN, pendingDLQ.getQueueArn());

        // Add alarm to send message to SNS if there are any messages on the dead letter queue
        createAlarmForDlq(stack, "PendingCompactionAlarm",
                "Alarms if there are any messages on the dead letter queue for the compactions queue",
                pendingDLQ, topic);
        errorMetrics.add(Utils.createErrorMetric("Pending Compaction Errors", pendingDLQ, instanceProperties));
        pendingQ.grantPurge(coreStacks.getPurgeQueuesPolicyForGrants());

        return pendingQ;
    }

    public Queue getCompactionJobsQueue() {
        return compactionJobsQueue;
    }
}
