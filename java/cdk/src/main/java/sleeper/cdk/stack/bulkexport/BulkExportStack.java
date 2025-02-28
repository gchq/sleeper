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

import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CommonProperty.ID;

@SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
public class BulkExportStack extends NestedStack {
    public static final String BULK_EXPORT_QUEUE_URL = "BulkExportQueueUrl";
    public static final String BULK_EXPORT_QUEUE_NAME = "BulkExportQueueName";
    public static final String BULK_EXPORT_QUEUE_DLQ_URL = "BulkExportQueueDlqUrl";
    public static final String BULK_EXPORT_QUEUE_DLQ_NAME = "BulkExportQueueDlqName";
    public static final String BULK_EXPORT_LAMBDA_ROLE_ARN = "BulkExportLambdaRoleArn";
    public static final String LEAF_PARTITION_BULK_EXPORT_QUEUE_URL = "LeafPartitionBulkExportQueueUrl";
    public static final String LEAF_PARTITION_BULK_EXPORT_QUEUE_NAME = "LeafPartitionBulkExportQueueName";
    public static final String LEAF_PARTITION_BULK_EXPORT_QUEUE_DLQ_URL = "LeafPartitionBulkExportQueueDlqUrl";
    public static final String LEAF_PARTITION_BULK_EXPORT_QUEUE_DLQ_NAME = "LeafPartitionBulkExportQueueDlqName";

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

        IFunction bulkExportLambda = lambdaCode.buildFunction(this, LambdaHandler.BULK_EXPORT, "BulkExportLambda",
                builder -> builder
                        .functionName(functionName)
                        .description("Sends a message to export for a leaf partition")
                        .memorySize(instanceProperties.getInt(BULK_EXPORT_LAMBDA_MEMORY_IN_MB))
                        .timeout(Duration.seconds(instanceProperties.getInt(
                                BULK_EXPORT_LAMBDA_TIMEOUT_IN_SECONDS)))
                        .environment(Utils.createDefaultEnvironment(instanceProperties))
                        .reservedConcurrentExecutions(1)
                        .logGroup(coreStacks.getLogGroup(LogGroupRef.BULK_EXPORT_SPITTER)));

        attachPolicy(bulkExportLambda, "BulkExportLambda");

        List<Queue> bulkExportQueues = createQueueAndDeadLetterQueue("BulkExport", instanceProperties);
        Queue bulkExportQ = bulkExportQueues.get(0);
        Queue bulkExportQueueQueryDlq = bulkExportQueues.get(1);
        setQueueOutputProps(instanceProperties, bulkExportQ, bulkExportQueueQueryDlq,
                QueueType.EXPORT);

        // Add the queue as a source of events for the lambdas
        SqsEventSourceProps eventSourceProps = SqsEventSourceProps.builder()
                .batchSize(1)
                .build();

        bulkExportLambda.addEventSource(new SqsEventSource(bulkExportQ, eventSourceProps));

        List<Queue> leafPartitionQueues = createQueueAndDeadLetterQueue("BulkExportLeafPartition", instanceProperties);
        Queue leafPartitionQueuesQ = leafPartitionQueues.get(0);
        Queue leafPartitionQueuesDlq = leafPartitionQueues.get(1);
        setQueueOutputProps(instanceProperties, leafPartitionQueuesQ, leafPartitionQueuesDlq, QueueType.LEAF_PARTITION);

        /*
         * Output the role of the lambda as a property so that clients that want the
         * results of queries written
         * to their own SQS queue can give the role permission to write to their queue
         */
        IRole bulkExportLambdaRole = Objects.requireNonNull(bulkExportLambda.getRole());
        instanceProperties.set(CdkDefinedInstanceProperty.BULK_EXPORT_LAMBDA_ROLE,
                bulkExportLambdaRole.getRoleName());

        CfnOutputProps bulkExportLambdaRoleOutputProps = new CfnOutputProps.Builder()
                .value(bulkExportLambda.getRole().getRoleArn())
                .exportName(instanceProperties.get(ID) + "-" + BULK_EXPORT_LAMBDA_ROLE_ARN)
                .build();
        new CfnOutput(this, BULK_EXPORT_LAMBDA_ROLE_ARN, bulkExportLambdaRoleOutputProps);
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
                                BULK_EXPORT_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
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
        EXPORT,
        LEAF_PARTITION
    }

    private void setQueueOutputProps(InstanceProperties instanceProperties, Queue queue, Queue dlQueue,
            QueueType queueType) {
        switch (queueType) {
            case LEAF_PARTITION -> {
                instanceProperties.set(CdkDefinedInstanceProperty.LEAF_PARTITION_BULK_EXPORT_QUEUE_URL,
                        queue.getQueueUrl());
                instanceProperties.set(CdkDefinedInstanceProperty.LEAF_PARTITION_BULK_EXPORT_QUEUE_DLQ_URL,
                        dlQueue.getQueueUrl());
                instanceProperties.set(CdkDefinedInstanceProperty.LEAF_PARTITION_BULK_EXPORT_QUEUE_ARN,
                        queue.getQueueArn());

                new CfnOutput(this, LEAF_PARTITION_BULK_EXPORT_QUEUE_NAME, new CfnOutputProps.Builder()
                        .value(queue.getQueueName())
                        .exportName(instanceProperties.get(ID) + "-" + LEAF_PARTITION_BULK_EXPORT_QUEUE_NAME)
                        .build());

                new CfnOutput(this, LEAF_PARTITION_BULK_EXPORT_QUEUE_DLQ_NAME, new CfnOutputProps.Builder()
                        .value(dlQueue.getQueueName())
                        .exportName(instanceProperties.get(ID) + "-" + LEAF_PARTITION_BULK_EXPORT_QUEUE_DLQ_NAME)
                        .build());

                new CfnOutput(this,
                        LEAF_PARTITION_BULK_EXPORT_QUEUE_URL, new CfnOutputProps.Builder()
                                .value(queue.getQueueUrl())
                                .exportName(instanceProperties.get(ID) + "-" + LEAF_PARTITION_BULK_EXPORT_QUEUE_URL)
                                .build());

                new CfnOutput(this,
                        LEAF_PARTITION_BULK_EXPORT_QUEUE_DLQ_URL, new CfnOutputProps.Builder()
                                .value(dlQueue.getQueueUrl())
                                .exportName(instanceProperties.get(ID) + "-" + LEAF_PARTITION_BULK_EXPORT_QUEUE_DLQ_URL)
                                .build());
            }

            case EXPORT -> {
                instanceProperties.set(CdkDefinedInstanceProperty.BULK_EXPORT_QUEUE_URL, queue.getQueueUrl());
                instanceProperties.set(CdkDefinedInstanceProperty.BULK_EXPORT_QUEUE_DLQ_URL,
                        dlQueue.getQueueUrl());
                instanceProperties.set(CdkDefinedInstanceProperty.BULK_EXPORT_QUEUE_ARN, queue.getQueueArn());

                new CfnOutput(this, BULK_EXPORT_QUEUE_NAME, new CfnOutputProps.Builder()
                        .value(queue.getQueueName())
                        .exportName(instanceProperties.get(ID) + "-" + BULK_EXPORT_QUEUE_NAME)
                        .build());

                new CfnOutput(this, BULK_EXPORT_QUEUE_DLQ_NAME, new CfnOutputProps.Builder()
                        .value(dlQueue.getQueueName())
                        .exportName(instanceProperties.get(ID) + "-" + BULK_EXPORT_QUEUE_DLQ_NAME)
                        .build());

                new CfnOutput(this, BULK_EXPORT_QUEUE_URL, new CfnOutputProps.Builder()
                        .value(queue.getQueueUrl())
                        .exportName(instanceProperties.get(ID) + "-" + BULK_EXPORT_QUEUE_URL)
                        .build());

                new CfnOutput(this, BULK_EXPORT_QUEUE_DLQ_URL, new CfnOutputProps.Builder()
                        .value(dlQueue.getQueueUrl())
                        .exportName(instanceProperties.get(ID) + "-" + BULK_EXPORT_QUEUE_DLQ_URL)
                        .build());
            }

            default -> throw new IllegalArgumentException("Unknown queue type: " + queueType);
        }
    }
}
