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
package sleeper.cdk.stack.ingest;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.PointInTimeRecoverySpecification;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.IQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.SleeperInstanceProps;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.deploy.SleeperScheduleRule;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStore;
import sleeper.ingest.batcher.store.DynamoDBIngestRequestFormat;

import java.util.List;
import java.util.Map;

import static sleeper.cdk.util.Utils.removalPolicy;
import static sleeper.core.properties.instance.BatcherProperty.INGEST_BATCHER_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES;
import static sleeper.core.properties.instance.BatcherProperty.INGEST_BATCHER_JOB_CREATION_MEMORY_IN_MB;
import static sleeper.core.properties.instance.BatcherProperty.INGEST_BATCHER_JOB_CREATION_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.BatcherProperty.INGEST_BATCHER_SUBMITTER_MEMORY_IN_MB;
import static sleeper.core.properties.instance.BatcherProperty.INGEST_BATCHER_SUBMITTER_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_JOB_CREATION_FUNCTION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_DLQ_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_REQUEST_FUNCTION;
import static sleeper.core.properties.instance.CommonProperty.ID;

@SuppressFBWarnings("MC_OVERRIDABLE_METHOD_CALL_IN_CONSTRUCTOR")
public class IngestBatcherStack extends NestedStack {

    private final IQueue submitQueue;

    public IngestBatcherStack(
            Construct scope, String id,
            SleeperInstanceProps props,
            SleeperCoreStacks coreStacks,
            IngestStacks ingestStacks) {
        super(scope, id);
        InstanceProperties instanceProperties = props.getInstanceProperties();
        String instanceId = Utils.cleanInstanceId(instanceProperties);

        // Queue to submit files to the batcher
        Queue submitDLQ = Queue.Builder
                .create(this, "IngestBatcherSubmitDLQ")
                .queueName(String.join("-", "sleeper", instanceId, "IngestBatcherSubmitDLQ"))
                .build();
        DeadLetterQueue ingestJobDeadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(submitDLQ)
                .build();
        submitQueue = Queue.Builder
                .create(this, "IngestBatcherSubmitQueue")
                .queueName(String.join("-", "sleeper", instanceId, "IngestBatcherSubmitQ"))
                .deadLetterQueue(ingestJobDeadLetterQueue)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(INGEST_BATCHER_SUBMITTER_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(INGEST_BATCHER_SUBMIT_QUEUE_URL, submitQueue.getQueueUrl());
        instanceProperties.set(INGEST_BATCHER_SUBMIT_QUEUE_ARN, submitQueue.getQueueArn());
        instanceProperties.set(INGEST_BATCHER_SUBMIT_DLQ_URL, submitDLQ.getQueueUrl());
        instanceProperties.set(INGEST_BATCHER_SUBMIT_DLQ_ARN, submitDLQ.getQueueArn());

        coreStacks.alarmOnDeadLetters(this, "IngestBatcherAlarm", "ingest batcher submission", submitDLQ);

        // DynamoDB table to track submitted files
        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);
        Table ingestRequestsTable = Table.Builder
                .create(this, "DynamoDBIngestBatcherRequestsTable")
                .tableName(DynamoDBIngestBatcherStore.ingestRequestsTableName(instanceProperties.get(ID)))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(Attribute.builder()
                        .name(DynamoDBIngestRequestFormat.JOB_ID)
                        .type(AttributeType.STRING)
                        .build())
                .sortKey(Attribute.builder()
                        .name(DynamoDBIngestRequestFormat.FILE_PATH)
                        .type(AttributeType.STRING)
                        .build())
                .timeToLiveAttribute(DynamoDBIngestRequestFormat.EXPIRY_TIME)
                .pointInTimeRecoverySpecification(PointInTimeRecoverySpecification.builder()
                        .pointInTimeRecoveryEnabled(false)
                        .build())
                .build();

        // Lambdas to receive submitted files and create batches
        SleeperLambdaCode lambdaCode = props.getArtefacts().lambdaCodeAtScope(this);

        String submitterName = String.join("-", "sleeper", instanceId, "ingest-batcher-submit-files");
        String jobCreatorName = String.join("-", "sleeper", instanceId, "ingest-batcher-create-jobs");

        Map<String, String> environmentVariables = EnvironmentUtils.createDefaultEnvironment(instanceProperties);

        IFunction submitterLambda = lambdaCode.buildFunction(this, LambdaHandler.INGEST_BATCHER_SUBMITTER, "SubmitToIngestBatcherLambda", builder -> builder
                .functionName(submitterName)
                .description("Triggered by an SQS event that contains a request to ingest a file")
                .memorySize(instanceProperties.getInt(INGEST_BATCHER_SUBMITTER_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(INGEST_BATCHER_SUBMITTER_TIMEOUT_IN_SECONDS)))
                .environment(environmentVariables)
                .logGroup(coreStacks.getLogGroup(LogGroupRef.INGEST_BATCHER_SUBMIT_FILES))
                .events(List.of(new SqsEventSource(submitQueue))));
        instanceProperties.set(INGEST_BATCHER_SUBMIT_REQUEST_FUNCTION, submitterLambda.getFunctionName());

        ingestRequestsTable.grantReadWriteData(submitterLambda);
        submitQueue.grantConsumeMessages(submitterLambda);
        submitDLQ.grantSendMessages(submitterLambda);
        coreStacks.grantReadTablesConfig(submitterLambda);
        coreStacks.grantReadTableDataBucket(submitterLambda);
        coreStacks.grantReadIngestSources(submitterLambda.getRole());

        IFunction jobCreatorLambda = lambdaCode.buildFunction(this, LambdaHandler.INGEST_BATCHER_JOB_CREATOR, "IngestBatcherJobCreationLambda", builder -> builder
                .functionName(jobCreatorName)
                .description("Create jobs by batching up submitted file ingest requests")
                .memorySize(instanceProperties.getInt(INGEST_BATCHER_JOB_CREATION_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(INGEST_BATCHER_JOB_CREATION_TIMEOUT_IN_SECONDS)))
                .environment(environmentVariables)
                .reservedConcurrentExecutions(1)
                .logGroup(coreStacks.getLogGroup(LogGroupRef.INGEST_BATCHER_CREATE_JOBS)));
        instanceProperties.set(INGEST_BATCHER_JOB_CREATION_FUNCTION, jobCreatorLambda.getFunctionName());

        ingestRequestsTable.grantReadWriteData(jobCreatorLambda);
        coreStacks.grantReadTablesConfig(jobCreatorLambda);
        ingestStacks.ingestQueues().forEach(queue -> queue.grantSendMessages(jobCreatorLambda));
        submitQueue.grantSendMessages(coreStacks.getIngestByQueuePolicyForGrants());
        ingestRequestsTable.grantReadData(coreStacks.getIngestByQueuePolicyForGrants());
        coreStacks.grantInvokeScheduled(jobCreatorLambda);

        // CloudWatch rule to trigger the batcher to create jobs from file ingest requests
        Rule rule = Rule.Builder
                .create(this, "IngestBatcherJobCreationPeriodicTrigger")
                .ruleName(SleeperScheduleRule.INGEST_BATCHER_JOB_CREATION.buildRuleName(instanceProperties))
                .description(SleeperScheduleRule.INGEST_BATCHER_JOB_CREATION.getDescription())
                .enabled(!props.isDeployPaused())
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(INGEST_BATCHER_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES))))
                .targets(List.of(new LambdaFunction(jobCreatorLambda)))
                .build();
        instanceProperties.set(INGEST_BATCHER_JOB_CREATION_CLOUDWATCH_RULE, rule.getRuleName());
        Utils.addTags(this, instanceProperties);
    }

    public IQueue getSubmitQueue() {
        return submitQueue;
    }
}
