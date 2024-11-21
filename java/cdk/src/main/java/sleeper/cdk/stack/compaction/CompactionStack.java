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
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.cloudwatch.IMetric;
import software.amazon.awscdk.services.ecs.Cluster;
import software.amazon.awscdk.services.ecs.Ec2TaskDefinition;
import software.amazon.awscdk.services.ecs.FargateTaskDefinition;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
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
import static sleeper.core.properties.instance.CommonProperty.TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB;
import static sleeper.core.properties.instance.CommonProperty.TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_BATCH_SIZE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_MAX_RETRIES;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;

/**
 * Deploys the resources needed to perform compaction jobs. Specifically, there is:
 * <p>
 * - A lambda, that is periodically triggered by a CloudWatch rule, to query the state store for
 * information about active files with no job id, to create compaction job definitions as
 * appropriate and post them to a queue.
 * - An ECS {@link Cluster} and either a {@link FargateTaskDefinition} or a {@link Ec2TaskDefinition}
 * for tasks that will perform compaction jobs.
 * - A lambda, that is periodically triggered by a CloudWatch rule, to look at the
 * size of the queue and the number of running tasks and create more tasks if necessary.
 */
public class CompactionStack extends NestedStack {
    public static final String COMPACTION_STACK_QUEUE_URL = "CompactionStackQueueUrlKey";
    public static final String COMPACTION_STACK_DLQ_URL = "CompactionStackDLQUrlKey";
    public static final String COMPACTION_CLUSTER_NAME = "CompactionClusterName";

    private Queue compactionJobQ;
    private Queue compactionDLQ;
    private final InstanceProperties instanceProperties;

    public CompactionStack(
            Construct scope,
            String id,
            InstanceProperties instanceProperties,
            BuiltJars jars,
            Topic topic,
            CoreStacks coreStacks,
            List<IMetric> errorMetrics) {
        super(scope, id);
        this.instanceProperties = instanceProperties;
        // The compaction stack consists of the following components:
        // - An SQS queue for the compaction jobs.
        // - A lambda to periodically check for compaction jobs that should be created.
        //   This lambda is fired periodically by a CloudWatch rule. It queries the
        //   StateStore for information about the current partitions and files,
        //   identifies files that should be compacted, creates a job definition
        //   and sends it to an SQS queue.
        // - An ECS cluster, task definition, etc., for compaction jobs.
        // - A lambda that periodically checks the number of running compaction tasks
        //   and if there are not enough (i.e. there is a backlog on the queue
        //   then it creates more tasks).

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());
        LambdaCode lambdaCode = jars.lambdaCode(jarsBucket);

        // SQS queue for the compaction jobs
        Queue compactionJobsQueue = sqsQueueForCompactionJobs(coreStacks, topic, errorMetrics);

        // Lambda to periodically check for compaction jobs that should be created
        lambdaToCreateCompactionJobsBatchedViaSQS(coreStacks, topic, errorMetrics, jarsBucket, lambdaCode, compactionJobsQueue);

        new CompactionTaskResources(this, instanceProperties, lambdaCode, jarsBucket,
                compactionJobsQueue, topic, coreStacks, errorMetrics);

        Utils.addStackTagIfSet(this, instanceProperties);
    }

    private Queue sqsQueueForCompactionJobs(CoreStacks coreStacks, Topic topic, List<IMetric> errorMetrics) {
        // Create queue for compaction job definitions
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String dlQueueName = String.join("-", "sleeper", instanceId, "CompactionJobDLQ");
        compactionDLQ = Queue.Builder
                .create(this, "CompactionJobDefinitionsDeadLetterQueue")
                .queueName(dlQueueName)
                .build();
        DeadLetterQueue compactionJobDefinitionsDeadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(instanceProperties.getInt(COMPACTION_JOB_MAX_RETRIES))
                .queue(compactionDLQ)
                .build();
        String queueName = String.join("-", "sleeper", instanceId, "CompactionJobQ");
        compactionJobQ = Queue.Builder
                .create(this, "CompactionJobDefinitionsQueue")
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
        createAlarmForDlq(this, "CompactionAlarm",
                "Alarms if there are any messages on the dead letter queue for the compactions queue",
                compactionDLQ, topic);
        errorMetrics.add(Utils.createErrorMetric("Compaction Errors", compactionDLQ, instanceProperties));
        compactionJobQ.grantPurge(coreStacks.getPurgeQueuesPolicyForGrants());

        CfnOutputProps compactionJobDefinitionsQueueProps = new CfnOutputProps.Builder()
                .value(compactionJobQ.getQueueUrl())
                .build();
        new CfnOutput(this, COMPACTION_STACK_QUEUE_URL, compactionJobDefinitionsQueueProps);
        CfnOutputProps compactionJobDefinitionsDLQueueProps = new CfnOutputProps.Builder()
                .value(compactionJobDefinitionsDeadLetterQueue.getQueue().getQueueUrl())
                .build();
        new CfnOutput(this, COMPACTION_STACK_DLQ_URL, compactionJobDefinitionsDLQueueProps);

        return compactionJobQ;
    }

    private void lambdaToCreateCompactionJobsBatchedViaSQS(
            CoreStacks coreStacks, Topic topic, List<IMetric> errorMetrics,
            IBucket jarsBucket, LambdaCode lambdaCode, Queue compactionJobsQueue) {

        // Function to create compaction jobs
        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);

        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String triggerFunctionName = String.join("-", "sleeper", instanceId, "compaction-job-creation-trigger");
        String functionName = String.join("-", "sleeper", instanceId, "compaction-job-creation-handler");

        IFunction triggerFunction = lambdaCode.buildFunction(this, LambdaHandler.COMPACTION_JOB_CREATOR_TRIGGER, "CompactionJobsCreationTrigger", builder -> builder
                .functionName(triggerFunctionName)
                .description("Create batches of tables and send requests to create compaction jobs for those batches")
                .memorySize(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS)))
                .environment(environmentVariables)
                .reservedConcurrentExecutions(1)
                .logGroup(coreStacks.getLogGroupByFunctionName(triggerFunctionName)));

        IFunction handlerFunction = lambdaCode.buildFunction(this, LambdaHandler.COMPACTION_JOB_CREATOR, "CompactionJobsCreationHandler", builder -> builder
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
        coreStacks.grantInvokeScheduled(triggerFunction, jobCreationQueue);
        coreStacks.grantCreateCompactionJobs(coreStacks.getInvokeCompactionPolicyForGrants());
        compactionJobsQueue.grantSendMessages(coreStacks.getInvokeCompactionPolicyForGrants());

        // Cloudwatch rule to trigger this lambda
        Rule rule = Rule.Builder
                .create(this, "CompactionJobCreationPeriodicTrigger")
                .ruleName(SleeperScheduleRule.COMPACTION_JOB_CREATION.buildRuleName(instanceProperties))
                .description("A rule to periodically trigger the compaction job creation lambda")
                .enabled(!shouldDeployPaused(this))
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(COMPACTION_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES))))
                .targets(List.of(new LambdaFunction(triggerFunction)))
                .build();
        instanceProperties.set(COMPACTION_JOB_CREATION_TRIGGER_LAMBDA_FUNCTION, triggerFunction.getFunctionName());
        instanceProperties.set(COMPACTION_JOB_CREATION_CLOUDWATCH_RULE, rule.getRuleName());
    }

    private Queue sqsQueueForCompactionJobCreation(CoreStacks coreStacks, Topic topic, List<IMetric> errorMetrics) {
        // Create queue for compaction job creation invocation
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        Queue deadLetterQueue = Queue.Builder
                .create(this, "CompactionJobCreationDLQ")
                .queueName(String.join("-", "sleeper", instanceId, "CompactionJobCreationDLQ.fifo"))
                .fifo(true)
                .build();
        Queue queue = Queue.Builder
                .create(this, "CompactionJobCreationQueue")
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

        createAlarmForDlq(this, "CompactionJobCreationBatchAlarm",
                "Alarms if there are any messages on the dead letter queue for compaction job creation",
                deadLetterQueue, topic);
        errorMetrics.add(Utils.createErrorMetric("Compaction Batching Errors", deadLetterQueue, instanceProperties));
        queue.grantPurge(coreStacks.getPurgeQueuesPolicyForGrants());
        return queue;
    }

    public Queue getCompactionJobsQueue() {
        return compactionJobQ;
    }

    public Queue getCompactionDeadLetterQueue() {
        return compactionDLQ;
    }
}
