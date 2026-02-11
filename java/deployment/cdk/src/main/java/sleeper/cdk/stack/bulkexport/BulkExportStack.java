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
package sleeper.cdk.stack.bulkexport;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSourceProps;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.s3.LifecycleRule;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.SleeperInstanceProps;
import sleeper.cdk.artefacts.SleeperJarVersionIdsCache;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static sleeper.cdk.util.Utils.removalPolicy;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_RESULTS_BUCKET_EXPIRY_IN_DAYS;
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

    public BulkExportStack(
            Construct scope, String id,
            SleeperInstanceProps props,
            SleeperCoreStacks coreStacks) {
        super(scope, id);
        InstanceProperties instanceProperties = props.getInstanceProperties();
        SleeperJarVersionIdsCache jars = props.getJars();

        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String functionName = String.join("-", "sleeper",
                instanceId, "bulk-export_planner");

        List<Queue> bulkExportQueues = createQueueAndDeadLetterQueue("BulkExport", instanceProperties);
        Queue bulkExportQ = bulkExportQueues.get(0);
        Queue bulkExportQueueQueryDlq = bulkExportQueues.get(1);
        setQueueOutputProps(instanceProperties, bulkExportQ, bulkExportQueueQueryDlq, QueueType.EXPORT);

        List<Queue> leafPartitionQueues = createQueueAndDeadLetterQueue("BulkExportLeafPartition", instanceProperties);
        Queue leafPartitionQueuesQ = leafPartitionQueues.get(0);
        Queue leafPartitionQueuesDlq = leafPartitionQueues.get(1);
        setQueueOutputProps(instanceProperties, leafPartitionQueuesQ, leafPartitionQueuesDlq, QueueType.LEAF_PARTITION);

        IBucket jarsBucket = jars.createJarsBucketReference(this, "JarsBucket");
        SleeperLambdaCode lambdaCode = jars.lambdaCode(jarsBucket);

        IFunction bulkExportLambda = lambdaCode.buildFunction(this, LambdaHandler.BULK_EXPORT_PLANNER,
                "BulkExportPlanner",
                builder -> builder
                        .functionName(functionName)
                        .description("Sends a message to export for a leaf partition")
                        .memorySize(instanceProperties.getInt(BULK_EXPORT_LAMBDA_MEMORY_IN_MB))
                        .timeout(Duration.seconds(instanceProperties.getInt(
                                BULK_EXPORT_LAMBDA_TIMEOUT_IN_SECONDS)))
                        .environment(EnvironmentUtils.createDefaultEnvironment(instanceProperties))
                        .reservedConcurrentExecutions(1)
                        .logGroup(coreStacks.getLogGroup(LogGroupRef.BULK_EXPORT)));

        coreStacks.grantReadTablesMetadata(bulkExportLambda);
        leafPartitionQueuesQ.grantSendMessages(bulkExportLambda);

        // Add the queue as a source of events for the lambdas
        SqsEventSourceProps eventSourceProps = SqsEventSourceProps.builder()
                .batchSize(1)
                .build();

        bulkExportLambda.addEventSource(new SqsEventSource(bulkExportQ, eventSourceProps));

        /*
         * Output the role arn of the lambda as a property so that clients that want the
         * results of queries written to their own SQS queue can give the role
         * permission to write to their queue
         */
        IRole bulkExportLambdaRole = Objects.requireNonNull(bulkExportLambda.getRole());
        instanceProperties.set(CdkDefinedInstanceProperty.BULK_EXPORT_LAMBDA_ROLE_ARN,
                bulkExportLambdaRole.getRoleArn());

        CfnOutputProps bulkExportLambdaRoleOutputProps = new CfnOutputProps.Builder()
                .value(bulkExportLambda.getRole().getRoleArn())
                .exportName(instanceProperties.get(ID) + "-" + BULK_EXPORT_LAMBDA_ROLE_ARN)
                .build();
        new CfnOutput(this, BULK_EXPORT_LAMBDA_ROLE_ARN, bulkExportLambdaRoleOutputProps);

        IBucket exportResultsBucket = setupExportBucket(instanceProperties, coreStacks, lambdaCode);
        new BulkExportTaskResources(this,
                props, coreStacks, lambdaCode, jarsBucket,
                leafPartitionQueuesQ, exportResultsBucket);
    }

    /**
     * Create a queue and a dead letter queue for the queue.
     *
     * @param  id                 the id of the queue
     * @param  instanceProperties the instance properties
     * @return                    the queue and the dead letter queue
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

    /**
     * Create the export results bucket.
     *
     * @param  instanceProperties the instance properties
     * @param  coreStacks         the core stacks
     * @param  lambdaCode         the lambda code
     * @return                    the export results bucket
     */
    private IBucket setupExportBucket(InstanceProperties instanceProperties, SleeperCoreStacks coreStacks,
            SleeperLambdaCode lambdaCode) {
        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);
        String bucketName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "bulk-export-results");
        Bucket exportBucket = Bucket.Builder
                .create(this, "BulkExportResultsBucket")
                .bucketName(bucketName)
                .versioned(false)
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .encryption(BucketEncryption.S3_MANAGED)
                .removalPolicy(removalPolicy)
                .lifecycleRules(Collections.singletonList(
                        LifecycleRule.builder().expiration(Duration.days(instanceProperties.getInt(
                                BULK_EXPORT_RESULTS_BUCKET_EXPIRY_IN_DAYS))).build()))
                .build();
        instanceProperties.set(CdkDefinedInstanceProperty.BULK_EXPORT_S3_BUCKET, exportBucket.getBucketName());

        if (removalPolicy == RemovalPolicy.DESTROY) {
            coreStacks.addAutoDeleteS3Objects(this, exportBucket);
        }

        return exportBucket;
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
                instanceProperties.set(CdkDefinedInstanceProperty.LEAF_PARTITION_BULK_EXPORT_QUEUE_DLQ_ARN,
                        dlQueue.getQueueArn());

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
                instanceProperties.set(CdkDefinedInstanceProperty.BULK_EXPORT_QUEUE_DLQ_ARN, dlQueue.getQueueArn());

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
