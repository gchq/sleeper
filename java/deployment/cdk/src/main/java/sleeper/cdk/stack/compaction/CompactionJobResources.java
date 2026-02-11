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
package sleeper.cdk.stack.compaction;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.iam.IGrantable;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;

import sleeper.cdk.SleeperInstanceProps;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.deploy.SleeperScheduleRule;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.List;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_COMMIT_DLQ_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_COMMIT_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_COMMIT_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_COMMIT_QUEUE_URL;
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
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_COMMIT_BATCHER_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_COMMIT_BATCHER_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_COMMIT_BATCHER_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_COMMIT_BATCHER_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_COMMIT_BATCHING_WINDOW_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_COMMIT_BATCH_SIZE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_COMMIT_MAX_RETRIES;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_DISPATCH_MAX_RETRIES;
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
import static sleeper.core.properties.instance.CompactionProperty.PENDING_COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.TableStateProperty.TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB;
import static sleeper.core.properties.instance.TableStateProperty.TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS;

/**
 * Deploys the resources needed to create and commit compaction jobs. Specifically, there are the following resources:
 * <ul>
 * <li>A Cloudwatch Rule that periodically triggers a lambda named sleeper-id-compaction-job-creation-trigger. The
 * purpose of this lambda is to identify online tables and send the table ids to an SQS FIFO queue.</li>
 * <li>This SQS FIFO queue triggers a lambda named sleeper-id-compaction-job-creation-handler. The purpose of this
 * lambda is to split any file references that need splitting and to create batches of compaction jobs in leaf
 * partitions. The lambda sends updates to the state store and sends batches of compaction jobs to an SQS queue
 * named sleeper-id-PendingCompactionJobBatchQ.</li>
 * <li>This queue triggers a lambda named sleeper-id-compaction-job-dispatcher that checks whether the state store
 * has been upated with these compaction jobs. If so then the jobs in the batch are sent to an SQS queue named
 * sleeper-id-CompactionJobQ; if not then the batch is returned to the queue for processing later.</li>
 * <li>Jobs will be processed from the compaction job queue by resources defined in CompactionTaskResources.</li>
 * </ul>
 */
public class CompactionJobResources {
    private static final String COMPACTION_STACK_QUEUE_URL = "CompactionStackQueueUrlKey";
    private static final String COMPACTION_STACK_DLQ_URL = "CompactionStackDLQUrlKey";

    private final Stack stack;
    private final SleeperInstanceProps props;
    private final InstanceProperties instanceProperties;
    private final Queue compactionJobsQueue;
    private final Queue commitBatcherQueue;

    public CompactionJobResources(Stack stack,
            SleeperInstanceProps props,
            SleeperLambdaCode lambdaCode,
            IBucket jarsBucket,
            SleeperCoreStacks coreStacks) {
        this.stack = stack;
        this.props = props;
        this.instanceProperties = props.getInstanceProperties();

        Queue pendingQueue = sqsQueueForCompactionJobBatches(coreStacks);
        commitBatcherQueue = sqsQueueForCompactionBatcher(coreStacks);
        compactionJobsQueue = sqsQueueForCompactionJobs(coreStacks);
        IFunction creationFunction = lambdaToCreateCompactionJobBatches(coreStacks, jarsBucket, lambdaCode, compactionJobsQueue);
        IFunction sendFunction = lambdaToSendCompactionJobBatches(coreStacks, lambdaCode, pendingQueue);
        lambdaToBatchUpCompactionCommits(coreStacks, lambdaCode, commitBatcherQueue);

        grantCreateCompactionJobs(coreStacks, jarsBucket, pendingQueue, creationFunction);
        grantCreateCompactionJobs(coreStacks, jarsBucket, pendingQueue, coreStacks.getInvokeCompactionPolicyForGrants());
        grantCreateCompactionJobs(coreStacks, jarsBucket, pendingQueue, sendFunction);
    }

    private Queue sqsQueueForCompactionJobs(SleeperCoreStacks coreStacks) {
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

        coreStacks.alarmOnDeadLetters(stack, "CompactionAlarm", "compaction jobs", compactionDLQ);
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

    private IFunction lambdaToCreateCompactionJobBatches(
            SleeperCoreStacks coreStacks, IBucket jarsBucket, SleeperLambdaCode lambdaCode, Queue compactionJobsQueue) {

        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String triggerFunctionName = String.join("-", "sleeper", instanceId, "compaction-job-creation-trigger");
        String functionName = String.join("-", "sleeper", instanceId, "compaction-job-creation-handler");
        Map<String, String> environmentVariables = EnvironmentUtils.createDefaultEnvironment(instanceProperties);

        IFunction triggerFunction = lambdaCode.buildFunction(stack, LambdaHandler.COMPACTION_JOB_CREATOR_TRIGGER, "CompactionJobsCreationTrigger", builder -> builder
                .functionName(triggerFunctionName)
                .description("Create batches of online tables and send requests to create compaction jobs for those batches")
                .memorySize(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS)))
                .environment(environmentVariables)
                .reservedConcurrentExecutions(1)
                .logGroup(coreStacks.getLogGroup(LogGroupRef.COMPACTION_JOB_CREATION_TRIGGER)));

        IFunction handlerFunction = lambdaCode.buildFunction(stack, LambdaHandler.COMPACTION_JOB_CREATOR, "CompactionJobsCreationHandler", builder -> builder
                .functionName(functionName)
                .description("Scan the state stores of the provided tables looking for compaction jobs to create")
                .memorySize(instanceProperties.getInt(COMPACTION_JOB_CREATION_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS)))
                .environment(environmentVariables)
                .reservedConcurrentExecutions(instanceProperties.getIntOrNull(COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_RESERVED))
                .logGroup(coreStacks.getLogGroup(LogGroupRef.COMPACTION_JOB_CREATION_HANDLER)));

        // Send messages from the trigger function to the handler function
        Queue jobCreationQueue = sqsQueueForCompactionJobCreation(coreStacks);
        handlerFunction.addEventSource(SqsEventSource.Builder.create(jobCreationQueue)
                .batchSize(instanceProperties.getInt(COMPACTION_JOB_CREATION_BATCH_SIZE))
                .maxConcurrency(instanceProperties.getIntOrNull(COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_MAXIMUM))
                .build());

        jobCreationQueue.grantSendMessages(triggerFunction);
        coreStacks.grantReadTablesStatus(triggerFunction);
        coreStacks.grantInvokeScheduled(triggerFunction, jobCreationQueue);

        // Cloudwatch rule to trigger stack lambda
        Rule rule = Rule.Builder
                .create(stack, "CompactionJobCreationPeriodicTrigger")
                .ruleName(SleeperScheduleRule.COMPACTION_JOB_CREATION.buildRuleName(instanceProperties))
                .description(SleeperScheduleRule.COMPACTION_JOB_CREATION.getDescription())
                .enabled(!props.isDeployPaused())
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(COMPACTION_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES))))
                .targets(List.of(new LambdaFunction(triggerFunction)))
                .build();
        instanceProperties.set(COMPACTION_JOB_CREATION_TRIGGER_LAMBDA_FUNCTION, triggerFunction.getFunctionName());
        instanceProperties.set(COMPACTION_JOB_CREATION_CLOUDWATCH_RULE, rule.getRuleName());

        return handlerFunction;
    }

    private void grantCreateCompactionJobs(SleeperCoreStacks coreStacks, IBucket jarsBucket, Queue pendingQueue, IGrantable grantee) {
        coreStacks.grantCreateCompactionJobs(grantee);
        jarsBucket.grantRead(grantee);
        compactionJobsQueue.grantSendMessages(grantee);
        pendingQueue.grantSendMessages(grantee);
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE") // Queue.getDeadLetterQueue is marked as nullable but will always be set
    private IFunction lambdaToSendCompactionJobBatches(
            SleeperCoreStacks coreStacks, SleeperLambdaCode lambdaCode, Queue pendingQueue) {

        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String functionName = String.join("-", "sleeper", instanceId, "compaction-job-dispatcher");

        IFunction function = lambdaCode.buildFunction(stack, LambdaHandler.COMPACTION_JOB_DISPATCHER, "CompactionJobDispatcher", builder -> builder
                .functionName(functionName)
                .description("Sends batches of compaction jobs created by the job creation lambda")
                .memorySize(instanceProperties.getInt(COMPACTION_JOB_DISPATCH_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(COMPACTION_JOB_DISPATCH_LAMBDA_TIMEOUT_IN_SECONDS)))
                .environment(EnvironmentUtils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(instanceProperties.getIntOrNull(COMPACTION_JOB_DISPATCH_LAMBDA_CONCURRENCY_RESERVED))
                .logGroup(coreStacks.getLogGroup(LogGroupRef.COMPACTION_JOB_DISPATCHER)));

        function.addEventSource(SqsEventSource.Builder.create(pendingQueue)
                .batchSize(1)
                .maxConcurrency(instanceProperties.getIntOrNull(COMPACTION_JOB_DISPATCH_LAMBDA_CONCURRENCY_MAXIMUM))
                .build());

        pendingQueue.getDeadLetterQueue().getQueue().grantSendMessages(function);
        return function;
    }

    private Queue sqsQueueForCompactionJobCreation(SleeperCoreStacks coreStacks) {
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

        coreStacks.alarmOnDeadLetters(stack, "CompactionJobCreationBatchAlarm", "compaction job creation triggers", deadLetterQueue);
        queue.grantPurge(coreStacks.getPurgeQueuesPolicyForGrants());
        return queue;
    }

    private Queue sqsQueueForCompactionJobBatches(SleeperCoreStacks coreStacks) {
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
                        .maxReceiveCount(instanceProperties.getInt(COMPACTION_DISPATCH_MAX_RETRIES))
                        .queue(pendingDLQ)
                        .build())
                .visibilityTimeout(
                        Duration.seconds(instanceProperties.getInt(PENDING_COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(COMPACTION_PENDING_QUEUE_URL, pendingQ.getQueueUrl());
        instanceProperties.set(COMPACTION_PENDING_QUEUE_ARN, pendingQ.getQueueArn());
        instanceProperties.set(COMPACTION_PENDING_DLQ_URL, pendingDLQ.getQueueUrl());
        instanceProperties.set(COMPACTION_PENDING_DLQ_ARN, pendingDLQ.getQueueArn());

        coreStacks.alarmOnDeadLetters(stack, "PendingCompactionAlarm", "pending compaction job batches", pendingDLQ);
        pendingQ.grantPurge(coreStacks.getPurgeQueuesPolicyForGrants());

        return pendingQ;
    }

    private void lambdaToBatchUpCompactionCommits(
            SleeperCoreStacks coreStacks, SleeperLambdaCode lambdaCode, Queue batcherQueue) {
        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "compaction-commit-batcher");

        IFunction function = lambdaCode.buildFunction(stack, LambdaHandler.COMPACTION_COMMIT_BATCHER, "CompactionCommitBatcher", builder -> builder
                .functionName(functionName)
                .description("Gathers up compaction commits and combines them into a larger update to the state store. " +
                        "Used when committing compaction jobs asynchronously.")
                .memorySize(instanceProperties.getInt(COMPACTION_COMMIT_BATCHER_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(COMPACTION_COMMIT_BATCHER_LAMBDA_TIMEOUT_IN_SECONDS)))
                .environment(EnvironmentUtils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(instanceProperties.getIntOrNull(COMPACTION_COMMIT_BATCHER_LAMBDA_CONCURRENCY_RESERVED))
                .logGroup(coreStacks.getLogGroup(LogGroupRef.COMPACTION_COMMIT_BATCHER)));

        function.addEventSource(SqsEventSource.Builder.create(batcherQueue)
                .batchSize(instanceProperties.getInt(COMPACTION_COMMIT_BATCH_SIZE))
                .maxBatchingWindow(Duration.seconds(instanceProperties.getInt(COMPACTION_COMMIT_BATCHING_WINDOW_IN_SECONDS)))
                .maxConcurrency(instanceProperties.getIntOrNull(COMPACTION_COMMIT_BATCHER_LAMBDA_CONCURRENCY_MAXIMUM))
                .build());
        coreStacks.grantSendStateStoreCommits(function);
    }

    private Queue sqsQueueForCompactionBatcher(SleeperCoreStacks coreStacks) {
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        Queue deadLetterQueue = Queue.Builder
                .create(stack, "CompactionCommitDLQ")
                .queueName(String.join("-", "sleeper", instanceId, "CompactionCommitDLQ"))
                .build();
        Queue queue = Queue.Builder
                .create(stack, "CompactionCommitQueue")
                .queueName(String.join("-", "sleeper", instanceId, "CompactionCommitQ"))
                .deadLetterQueue(DeadLetterQueue.builder()
                        .maxReceiveCount(instanceProperties.getInt(COMPACTION_COMMIT_MAX_RETRIES))
                        .queue(deadLetterQueue)
                        .build())
                .visibilityTimeout(
                        Duration.seconds(instanceProperties.getInt(COMPACTION_COMMIT_BATCHER_LAMBDA_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(COMPACTION_COMMIT_QUEUE_URL, queue.getQueueUrl());
        instanceProperties.set(COMPACTION_COMMIT_QUEUE_ARN, queue.getQueueArn());
        instanceProperties.set(COMPACTION_COMMIT_DLQ_URL, deadLetterQueue.getQueueUrl());
        instanceProperties.set(COMPACTION_COMMIT_DLQ_ARN, deadLetterQueue.getQueueArn());

        queue.grantPurge(coreStacks.getPurgeQueuesPolicyForGrants());
        coreStacks.alarmOnDeadLetters(stack, "CompactionCommitAlarm", "compaction commits", deadLetterQueue);
        return queue;
    }

    public Queue getCompactionJobsQueue() {
        return compactionJobsQueue;
    }

    public Queue getCommitBatcherQueue() {
        return commitBatcherQueue;
    }
}
