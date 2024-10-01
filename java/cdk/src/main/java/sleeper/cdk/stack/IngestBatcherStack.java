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
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.cloudwatch.IMetric;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.IQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.jars.BuiltJar;
import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.cdk.util.Utils;
import sleeper.core.properties.deploy.SleeperScheduleRule;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStore;
import sleeper.ingest.batcher.store.DynamoDBIngestRequestFormat;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static sleeper.cdk.util.Utils.createAlarmForDlq;
import static sleeper.cdk.util.Utils.createLambdaLogGroup;
import static sleeper.cdk.util.Utils.removalPolicy;
import static sleeper.cdk.util.Utils.shouldDeployPaused;
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
import static sleeper.core.properties.instance.CommonProperty.QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;

public class IngestBatcherStack extends NestedStack {

    private final IQueue submitQueue;

    public IngestBatcherStack(
            Construct scope,
            String id,
            InstanceProperties instanceProperties,
            BuiltJars jars,
            Topic topic,
            CoreStacks coreStacks,
            IngestStacks ingestStacks,
            List<IMetric> errorMetrics) {
        super(scope, id);
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
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(INGEST_BATCHER_SUBMIT_QUEUE_URL, submitQueue.getQueueUrl());
        instanceProperties.set(INGEST_BATCHER_SUBMIT_QUEUE_ARN, submitQueue.getQueueArn());
        instanceProperties.set(INGEST_BATCHER_SUBMIT_DLQ_URL, submitDLQ.getQueueUrl());
        instanceProperties.set(INGEST_BATCHER_SUBMIT_DLQ_ARN, submitDLQ.getQueueArn());

        createAlarmForDlq(this, "IngestBatcherAlarm",
                "Alarms if there are any messages on the dead letter queue for the ingest batcher queue",
                submitDLQ, topic);
        errorMetrics.add(Utils.createErrorMetric("Ingest Batcher Errors", submitDLQ, instanceProperties));
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
                .pointInTimeRecovery(false)
                .build();

        // Lambdas to receive submitted files and create batches
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());
        LambdaCode submitterJar = jars.lambdaCode(BuiltJar.INGEST_BATCHER_SUBMITTER, jarsBucket);
        LambdaCode jobCreatorJar = jars.lambdaCode(BuiltJar.INGEST_BATCHER_JOB_CREATOR, jarsBucket);

        String submitterName = String.join("-", "sleeper", instanceId, "ingest-batcher-submit-files");
        String jobCreatorName = String.join("-", "sleeper", instanceId, "ingest-batcher-create-jobs");

        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);

        IFunction submitterLambda = submitterJar.buildFunction(this, "SubmitToIngestBatcherLambda", builder -> builder
                .functionName(submitterName)
                .description("Triggered by an SQS event that contains a request to ingest a file")
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .memorySize(instanceProperties.getInt(INGEST_BATCHER_SUBMITTER_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(INGEST_BATCHER_SUBMITTER_TIMEOUT_IN_SECONDS)))
                .handler("sleeper.ingest.batcher.submitter.IngestBatcherSubmitterLambda::handleRequest")
                .environment(environmentVariables)
                .logGroup(createLambdaLogGroup(this, "SubmitToIngestBatcherLogGroup", submitterName, instanceProperties))
                .events(List.of(new SqsEventSource(submitQueue))));
        instanceProperties.set(INGEST_BATCHER_SUBMIT_REQUEST_FUNCTION, submitterLambda.getFunctionName());

        ingestRequestsTable.grantReadWriteData(submitterLambda);
        submitQueue.grantConsumeMessages(submitterLambda);
        coreStacks.grantReadTablesConfig(submitterLambda);
        coreStacks.grantReadIngestSources(submitterLambda.getRole());

        IFunction jobCreatorLambda = jobCreatorJar.buildFunction(this, "IngestBatcherJobCreationLambda", builder -> builder
                .functionName(jobCreatorName)
                .description("Create jobs by batching up submitted file ingest requests")
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .memorySize(instanceProperties.getInt(INGEST_BATCHER_JOB_CREATION_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(INGEST_BATCHER_JOB_CREATION_TIMEOUT_IN_SECONDS)))
                .handler("sleeper.ingest.batcher.job.creator.IngestBatcherJobCreatorLambda::eventHandler")
                .environment(environmentVariables)
                .reservedConcurrentExecutions(1)
                .logGroup(createLambdaLogGroup(this, "IngestBatcherJobCreationLogGroup", jobCreatorName, instanceProperties)));
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
                .description("A rule to periodically trigger the ingest batcher job creation lambda")
                .enabled(!shouldDeployPaused(this))
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(INGEST_BATCHER_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES))))
                .targets(Collections.singletonList(new LambdaFunction(jobCreatorLambda)))
                .build();
        instanceProperties.set(INGEST_BATCHER_JOB_CREATION_CLOUDWATCH_RULE, rule.getRuleName());
    }

    public IQueue getSubmitQueue() {
        return submitQueue;
    }
}
