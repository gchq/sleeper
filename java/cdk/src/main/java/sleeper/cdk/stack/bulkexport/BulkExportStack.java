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
import java.util.Objects;

import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_PROCESSOR_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_PROCESSOR_LAMBDA_TIMEOUT;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_PROCESSOR_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CommonProperty.ID;

@SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
public class BulkExportStack extends NestedStack {
    public static final String BULK_EXPORT_PROCESSOR_QUEUE_URL = "BulkExportProcessorQueueUrl";
    public static final String BULK_EXPORT_PROCESSOR_QUEUE_NAME = "BulkExportProcessorQueueName";
    public static final String BULK_EXPORT_PROCESSOR_LAMBDA_ROLE_ARN = "BulkExportProcessorLambdaRoleArn";

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

        IFunction lambda = lambdaCode.buildFunction(this, LambdaHandler.BULK_EXPORT, "BulkExportProcessorLambda",
                builder -> builder
                        .functionName(functionName)
                        .description("Sends a message to export from a leaf partition")
                        .memorySize(instanceProperties.getInt(BULK_EXPORT_PROCESSOR_LAMBDA_MEMORY_IN_MB))
                        .timeout(Duration.seconds(instanceProperties.getInt(
                                BULK_EXPORT_PROCESSOR_LAMBDA_TIMEOUT)))
                        .environment(Utils.createDefaultEnvironment(instanceProperties))
                        .reservedConcurrentExecutions(1)
                        .logGroup(coreStacks.getLogGroup(LogGroupRef.BULK_EXPORT_PROCESSOR)));

        attachPolicy(lambda, "BulkExport");

        String dlBulkExportProcessorQueueName = String.join("-", "sleeper", instanceId, "BulkExportProcessorDLQ");
        Queue bulkExportProcessorQueueQueryDlq = Queue.Builder
                .create(this, "BulkExportProcessorQueueDeadLetterQueue")
                .queueName(dlBulkExportProcessorQueueName)
                .build();
        DeadLetterQueue bulkExportProcessorDeadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(bulkExportProcessorQueueQueryDlq)
                .build();

        // Add the queue as a source of events for the lambdas
        SqsEventSourceProps eventSourceProps = SqsEventSourceProps.builder()
                .batchSize(1)
                .build();

        String queueName = String.join("-", "sleeper", instanceId, "BulkExportProcessorQ");
        Queue bulkExportProcessorQ = Queue.Builder
                .create(this, "BulkExportProcessorQueue")
                .queueName(queueName)
                .deadLetterQueue(bulkExportProcessorDeadLetterQueue)
                .visibilityTimeout(
                        Duration.seconds(instanceProperties.getInt(
                                BULK_EXPORT_PROCESSOR_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(CdkDefinedInstanceProperty.BULK_EXPORT_PROCESSOR_QUEUE_URL,
                bulkExportProcessorQ.getQueueUrl());
        instanceProperties.set(CdkDefinedInstanceProperty.BULK_EXPORT_PROCESSOR_QUEUE_ARN,
                bulkExportProcessorQ.getQueueArn());

        CfnOutputProps sourceQueueOutputNameProps = new CfnOutputProps.Builder()
                .value(bulkExportProcessorQ.getQueueName())
                .exportName(instanceProperties.get(ID) + "-" + BULK_EXPORT_PROCESSOR_QUEUE_NAME)
                .build();
        new CfnOutput(this, BULK_EXPORT_PROCESSOR_QUEUE_NAME, sourceQueueOutputNameProps);

        CfnOutputProps sourceQueueOutputProps = new CfnOutputProps.Builder()
                .value(bulkExportProcessorQ.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + BULK_EXPORT_PROCESSOR_QUEUE_URL)
                .build();
        new CfnOutput(this, BULK_EXPORT_PROCESSOR_QUEUE_URL, sourceQueueOutputProps);

        lambda.addEventSource(new SqsEventSource(bulkExportProcessorQ, eventSourceProps));

        /*
         * Output the role of the lambda as a property so that clients that want the
         * results of queries written
         * to their own SQS queue can give the role permission to write to their queue
         */
        IRole role = Objects.requireNonNull(lambda.getRole());
        CfnOutputProps bulkExportProcessorLambdaRoleOutputProps = new CfnOutputProps.Builder()
                .value(lambda.getRole().getRoleArn())
                .exportName(instanceProperties.get(ID) + "-" + BULK_EXPORT_PROCESSOR_LAMBDA_ROLE_ARN)
                .build();
        new CfnOutput(this, BULK_EXPORT_PROCESSOR_LAMBDA_ROLE_ARN, bulkExportProcessorLambdaRoleOutputProps);
        instanceProperties.set(CdkDefinedInstanceProperty.BULK_EXPORT_PROCESSOR_LAMBDA_ROLE, role.getRoleName());
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
        String policyName = "SendToAnySQSPolicy" + id;
        Policy policy = new Policy(this, policyName);
        policy.addStatements(policyStatement);
        Objects.requireNonNull(lambda.getRole()).attachInlinePolicy(policy);
    }
}
