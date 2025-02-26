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
package sleeper.cdk.stack.bulkexport;

import com.amazonaws.auth.policy.actions.DynamoDBv2Actions;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.auth.policy.actions.SQSActions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.Policy;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.PolicyStatementProps;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSourceProps;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.cdk.stack.core.CoreStacks;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_PROCESSOR_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_PROCESSOR_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CommonProperty.ID;

@SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
public class BulkExportStack extends NestedStack {
    public static final String BULK_EXPORT_PROCESSOR_QUEUE_URL = "BulkExportProcessorQueueUrl";
    public static final String BULK_EXPORT_PROCESSOR_QUEUE_NAME = "BulkExportProcessorQueueName";
    public static final String BULK_EXPORT_PROCESSOR_QUEUE_DLQ_URL = "BulkExportProcessorQueueDlqUrl";
    public static final String BULK_EXPORT_PROCESSOR_QUEUE_DLQ_NAME = "BulkExportProcessorQueueDlqName";
    public static final String BULK_EXPORT_PROCESSOR_LAMBDA_ROLE_ARN = "BulkExportProcessorLambdaRoleArn";
    public static final String BULK_EXPORT_SPLITTER_QUEUE_URL = "BulkExportSplitterQueueUrl";
    public static final String BULK_EXPORT_SPLITTER_QUEUE_NAME = "BulkExportSplitterQueueName";
    public static final String BULK_EXPORT_SPLITTER_QUEUE_DLQ_URL = "BulkExportSplitterQueueDlqUrl";
    public static final String BULK_EXPORT_SPLITTER_QUEUE_DLQ_NAME = "BulkExportSplitterQueueDlqName";
    public static final String BULK_EXPORT_SPLITTER_LAMBDA_ROLE_ARN = "BulkExportSplitterLambdaRoleArn";

    public BulkExportStack(Construct scope,
            String id,
            InstanceProperties instanceProperties,
            BuiltJars jars,
            CoreStacks coreStacks) {
        super(scope, id);

        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String functionName = String.join("-", "sleeper",
                instanceId, "bulk-export");

        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());
        LambdaCode lambdaCode = jars.lambdaCode(jarsBucket);

        IFunction splitterLambda = lambdaCode.buildFunction(this, LambdaHandler.BULK_EXPORT, "BulkExportSplitterLambda",
                builder -> builder
                        .functionName(String.join("-", functionName, "splitter"))
                        .description("Sends a message to export from a leaf partition")
                        .memorySize(instanceProperties.getInt(BULK_EXPORT_PROCESSOR_LAMBDA_MEMORY_IN_MB))
                        .timeout(Duration.seconds(instanceProperties.getInt(
                                BULK_EXPORT_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS)))
                        .environment(Utils.createDefaultEnvironment(instanceProperties))
                        .reservedConcurrentExecutions(1)
                        .logGroup(coreStacks.getLogGroup(LogGroupRef.BULK_EXPORT_SPITTER)));

        attachPolicy(splitterLambda, "BulkExportSplitter");

        List<Queue> splitterQueues = createQueueAndDeadLetterQueue("BulkExport", instanceProperties);
        Queue bulkExportSplitterQ = splitterQueues.get(0);
        Queue bulkExportSplitterQueueQueryDlq = splitterQueues.get(1);
        setQueueOutputProps(instanceProperties, bulkExportSplitterQ, bulkExportSplitterQueueQueryDlq, QueueType.SPLITTER);

        // Add the queue as a source of events for the lambdas
        SqsEventSourceProps eventSourceProps = SqsEventSourceProps.builder()
                .batchSize(1)
                .build();

        splitterLambda.addEventSource(new SqsEventSource(bulkExportSplitterQ, eventSourceProps));

        List<Queue> processorQueues = createQueueAndDeadLetterQueue("BulkExportProcessor", instanceProperties);
        Queue bulkExportProcessorQ = processorQueues.get(0);
        Queue bulkExportProcessorDlq = processorQueues.get(1);
        setQueueOutputProps(instanceProperties, bulkExportProcessorQ, bulkExportProcessorDlq, QueueType.PROCESSOR);

        IFunction processorLambda = lambdaCode.buildFunction(this, LambdaHandler.BULK_EXPORT, "BulkExportProcessorLambda",
                builder -> builder
                        .functionName(String.join("-", functionName, "processor"))
                        .description("Does the bulk export for a leaf partition")
                        .memorySize(instanceProperties.getInt(BULK_EXPORT_PROCESSOR_LAMBDA_MEMORY_IN_MB))
                        .timeout(Duration.seconds(instanceProperties.getInt(
                                BULK_EXPORT_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS)))
                        .environment(Utils.createDefaultEnvironment(instanceProperties))
                        .reservedConcurrentExecutions(1)
                        .logGroup(coreStacks.getLogGroup(LogGroupRef.BULK_EXPORT_PROCESSOR)));

        attachPolicy(processorLambda, "BulkExport");

        processorLambda.addEventSource(new SqsEventSource(bulkExportProcessorQ, eventSourceProps));

        /*
         * Output the role of the lambda as a property so that clients that want the
         * results of queries written
         * to their own SQS queue can give the role permission to write to their queue
         */
        IRole splitterRole = Objects.requireNonNull(splitterLambda.getRole());
        instanceProperties.set(CdkDefinedInstanceProperty.BULK_EXPORT_SPLITTER_LAMBDA_ROLE,
                splitterRole.getRoleName());

        IRole processorRole = Objects.requireNonNull(processorLambda.getRole());
        instanceProperties.set(CdkDefinedInstanceProperty.BULK_EXPORT_PROCESSOR_LAMBDA_ROLE,
                processorRole.getRoleName());

        CfnOutputProps bulkExportSplitterLambdaRoleOutputProps = new CfnOutputProps.Builder()
                .value(splitterLambda.getRole().getRoleArn())
                .exportName(instanceProperties.get(ID) + "-" + BULK_EXPORT_SPLITTER_LAMBDA_ROLE_ARN)
                .build();
        new CfnOutput(this, BULK_EXPORT_SPLITTER_LAMBDA_ROLE_ARN, bulkExportSplitterLambdaRoleOutputProps);

        CfnOutputProps bulkExportProcessorLambdaRoleOutputProps = new CfnOutputProps.Builder()
                .value(processorLambda.getRole().getRoleArn())
                .exportName(instanceProperties.get(ID) + "-" + BULK_EXPORT_PROCESSOR_LAMBDA_ROLE_ARN)
                .build();
        new CfnOutput(this, BULK_EXPORT_PROCESSOR_LAMBDA_ROLE_ARN, bulkExportProcessorLambdaRoleOutputProps);
    }

    /**
     * Create a queue and a dead letter queue for the queue.
     *
     * @param id                 the id of the queue
     * @param instanceProperties the instance properties
     * @return the queue and the dead letter queue
     */
    private List<Queue> createQueueAndDeadLetterQueue(String id, InstanceProperties instanceProperties) {
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String queueNameDLQ = String.join("-", "sleeper", instanceId, id, "DLQ");
        Queue queueDLQ = Queue.Builder
                .create(this, id + "DeadLetterQueue")
                .queueName(queueNameDLQ)
                .build();
        DeadLetterQueue deadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(queueDLQ)
                .build();

        String queueName = String.join("-", "sleeper", instanceId, id, "Q");
        Queue queue = Queue.Builder
                .create(this, id + "Queue")
                .queueName(queueName)
                .deadLetterQueue(deadLetterQueue)
                .visibilityTimeout(
                        Duration.seconds(instanceProperties.getInt(
                                BULK_EXPORT_PROCESSOR_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
                .build();
        return Arrays.asList(queue, queueDLQ);
    }

    private void attachPolicy(IFunction lambda, String id) {
        PolicyStatementProps policyStatementProps = PolicyStatementProps.builder()
                .effect(Effect.ALLOW)
                .actions(
                        Arrays.asList(SQSActions.SendMessage.getActionName(),
                                SQSActions.ReceiveMessage.getActionName(),
                                S3Actions.PutObject.getActionName(),
                                S3Actions.GetObject.getActionName(),
                                DynamoDBv2Actions.Query.getActionName()))
                .resources(Collections.singletonList("*"))
                .build();
        PolicyStatement policyStatement = new PolicyStatement(policyStatementProps);
        String policyName = "BulkExportPolicy" + id;
        Policy policy = new Policy(this, policyName);
        policy.addStatements(policyStatement);
        Objects.requireNonNull(lambda.getRole()).attachInlinePolicy(policy);
    }

    public enum QueueType {
        SPLITTER,
        PROCESSOR
    }

    private void setQueueOutputProps(InstanceProperties instanceProperties, Queue queue, Queue dlQueue, QueueType queueType) {
        switch (queueType) {
            case SPLITTER:
                instanceProperties.set(CdkDefinedInstanceProperty.BULK_EXPORT_SPLITTER_QUEUE_URL, queue.getQueueUrl());
                instanceProperties.set(CdkDefinedInstanceProperty.BULK_EXPORT_SPLITTER_QUEUE_DLQ_URL, dlQueue.getQueueUrl());
                instanceProperties.set(CdkDefinedInstanceProperty.BULK_EXPORT_SPLITTER_QUEUE_ARN, queue.getQueueArn());

                new CfnOutput(this, BULK_EXPORT_SPLITTER_QUEUE_NAME, new CfnOutputProps.Builder()
                        .value(queue.getQueueName())
                        .exportName(instanceProperties.get(ID) + "-" + BULK_EXPORT_SPLITTER_QUEUE_NAME)
                        .build());

                new CfnOutput(this, BULK_EXPORT_SPLITTER_QUEUE_DLQ_NAME, new CfnOutputProps.Builder()
                        .value(dlQueue.getQueueName())
                        .exportName(instanceProperties.get(ID) + "-" + BULK_EXPORT_SPLITTER_QUEUE_DLQ_NAME)
                        .build());

                new CfnOutput(this, BULK_EXPORT_SPLITTER_QUEUE_URL, new CfnOutputProps.Builder()
                        .value(queue.getQueueUrl())
                        .exportName(instanceProperties.get(ID) + "-" + BULK_EXPORT_SPLITTER_QUEUE_URL)
                        .build());

                new CfnOutput(this, BULK_EXPORT_SPLITTER_QUEUE_DLQ_URL, new CfnOutputProps.Builder()
                        .value(dlQueue.getQueueUrl())
                        .exportName(instanceProperties.get(ID) + "-" + BULK_EXPORT_SPLITTER_QUEUE_DLQ_URL)
                        .build());
                break;

            case PROCESSOR:
                instanceProperties.set(CdkDefinedInstanceProperty.BULK_EXPORT_PROCESSOR_QUEUE_URL, queue.getQueueUrl());
                instanceProperties.set(CdkDefinedInstanceProperty.BULK_EXPORT_PROCESSOR_QUEUE_DLQ_URL, dlQueue.getQueueUrl());
                instanceProperties.set(CdkDefinedInstanceProperty.BULK_EXPORT_PROCESSOR_QUEUE_ARN, queue.getQueueArn());

                new CfnOutput(this, BULK_EXPORT_PROCESSOR_QUEUE_NAME, new CfnOutputProps.Builder()
                        .value(queue.getQueueName())
                        .exportName(instanceProperties.get(ID) + "-" + BULK_EXPORT_PROCESSOR_QUEUE_NAME)
                        .build());

                new CfnOutput(this, BULK_EXPORT_PROCESSOR_QUEUE_DLQ_NAME, new CfnOutputProps.Builder()
                        .value(dlQueue.getQueueName())
                        .exportName(instanceProperties.get(ID) + "-" + BULK_EXPORT_PROCESSOR_QUEUE_DLQ_NAME)
                        .build());

                new CfnOutput(this, BULK_EXPORT_PROCESSOR_QUEUE_URL, new CfnOutputProps.Builder()
                        .value(queue.getQueueUrl())
                        .exportName(instanceProperties.get(ID) + "-" + BULK_EXPORT_PROCESSOR_QUEUE_URL)
                        .build());

                new CfnOutput(this, BULK_EXPORT_PROCESSOR_QUEUE_DLQ_URL, new CfnOutputProps.Builder()
                        .value(dlQueue.getQueueUrl())
                        .exportName(instanceProperties.get(ID) + "-" + BULK_EXPORT_PROCESSOR_QUEUE_DLQ_URL)
                        .build());
                break;

            default:
                throw new IllegalArgumentException("Unknown queue type: " + queueType);
        }
    }
}
