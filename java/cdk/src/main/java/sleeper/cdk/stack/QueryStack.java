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

import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.auth.policy.actions.SQSActions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.cloudwatch.IMetric;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.ITable;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.Policy;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.PolicyStatementProps;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSourceProps;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.s3.LifecycleRule;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.IQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.cdk.util.AutoDeleteS3Objects;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static sleeper.cdk.util.Utils.createAlarmForDlq;
import static sleeper.cdk.util.Utils.removalPolicy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_TRACKER_TABLE_NAME;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.QueryProperty.QUERY_PROCESSOR_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.QueryProperty.QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.QueryProperty.QUERY_RESULTS_BUCKET_EXPIRY_IN_DAYS;

/**
 * Deploys resources to run queries. This consists of lambda {@link Function}s to
 * process them and {@link Queue}s to take queries and post results.
 */
@SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
public class QueryStack extends NestedStack {
    public static final String LEAF_PARTITION_QUERY_QUEUE_NAME = "LeafPartitionQueryQueueName";
    public static final String LEAF_PARTITION_QUERY_QUEUE_URL = "LeafPartitionQueryQueueUrl";
    public static final String LEAF_PARTITION_QUERY_DLQ_URL = "LeafPartitionQueryDLQUrl";
    public static final String QUERY_RESULTS_QUEUE_NAME = "QueryResultsQueueName";
    public static final String QUERY_RESULTS_QUEUE_URL = "QueryResultsQueueUrl";
    public static final String QUERY_LAMBDA_ROLE_ARN = "QueryLambdaRoleArn";

    private IFunction queryExecutorLambda;
    private IFunction leafPartitionQueryLambda;

    public QueryStack(Construct scope,
            String id,
            InstanceProperties instanceProperties,
            BuiltJars jars,
            Topic topic,
            CoreStacks coreStacks,
            QueryQueueStack queryQueueStack,
            List<IMetric> errorMetrics) {
        super(scope, id);

        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());
        LambdaCode lambdaCode = jars.lambdaCode(jarsBucket);

        String tableName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "query-tracking-table");

        Table queryTrackingTable = Table.Builder.create(this, "QueryTrackingTable")
                .tableName(tableName)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .timeToLiveAttribute("expiryDate")
                .removalPolicy(RemovalPolicy.DESTROY)
                .partitionKey(Attribute.builder()
                        .name("queryId")
                        .type(AttributeType.STRING)
                        .build())
                .sortKey(Attribute.builder()
                        .name("subQueryId")
                        .type(AttributeType.STRING)
                        .build())
                .build();

        instanceProperties.set(QUERY_TRACKER_TABLE_NAME, queryTrackingTable.getTableName());

        queryExecutorLambda = setupQueryExecutorLambda(coreStacks, queryQueueStack, instanceProperties, lambdaCode, jarsBucket, queryTrackingTable);
        leafPartitionQueryLambda = setupLeafPartitionQueryQueueAndLambda(coreStacks, instanceProperties, topic, lambdaCode, jarsBucket, queryTrackingTable, errorMetrics);
        Utils.addStackTagIfSet(this, instanceProperties);
    }

    private IFunction createFunction(
            String id, CoreStacks coreStacks, InstanceProperties instanceProperties, LambdaCode lambdaCode, LambdaHandler handler,
            String functionName, String description) {
        return lambdaCode.buildFunction(this, handler, id, builder -> builder
                .functionName(functionName)
                .description(description)
                .memorySize(instanceProperties.getInt(QUERY_PROCESSOR_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS)))
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .logGroup(coreStacks.getLogGroupByFunctionName(functionName)));
    }

    private IFunction setupQueryExecutorLambda(CoreStacks coreStacks, QueryQueueStack queryQueueStack, InstanceProperties instanceProperties, LambdaCode lambdaCode,
            IBucket jarsBucket, ITable queryTrackingTable) {
        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "query-executor");
        IFunction lambda = createFunction("QueryExecutorLambda", coreStacks, instanceProperties, lambdaCode, LambdaHandler.QUERY_EXECUTOR, functionName,
                "When a query arrives on the query SQS queue, this lambda is invoked to look for leaf partition queries");

        attachPolicy(lambda, "Query");

        // Add the queue as a source of events for the lambdas
        SqsEventSourceProps eventSourceProps = SqsEventSourceProps.builder()
                .batchSize(1)
                .build();

        lambda.addEventSource(new SqsEventSource(queryQueueStack.getQueue(), eventSourceProps));

        setPermissionsForLambda(coreStacks, jarsBucket, lambda, queryTrackingTable, queryQueueStack.getQueue());

        /*
         * Output the role of the lambda as a property so that clients that want the results of queries written
         * to their own SQS queue can give the role permission to write to their queue
         */
        CfnOutputProps queryLambdaRoleOutputProps = new CfnOutputProps.Builder()
                .value(Objects.requireNonNull(lambda.getRole()).getRoleArn())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_LAMBDA_ROLE_ARN)
                .build();
        new CfnOutput(this, QUERY_LAMBDA_ROLE_ARN, queryLambdaRoleOutputProps);

        IRole role = Objects.requireNonNull(lambda.getRole());
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_LAMBDA_ROLE, role.getRoleName());

        return lambda;
    }

    private IFunction setupLeafPartitionQueryQueueAndLambda(
            CoreStacks coreStacks, InstanceProperties instanceProperties, Topic topic,
            LambdaCode lambdaCode, IBucket jarsBucket, ITable queryTrackingTable, List<IMetric> errorMetrics) {
        Queue leafPartitionQueryQueue = setupLeafPartitionQueryQueue(instanceProperties, topic, errorMetrics);
        Queue queryResultsQueue = setupResultsQueue(instanceProperties);
        IBucket queryResultsBucket = setupResultsBucket(instanceProperties, coreStacks, lambdaCode);
        String leafQueryFunctionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "query-leaf-partition");
        IFunction lambda = createFunction("QueryLeafPartitionExecutorLambda", coreStacks, instanceProperties, lambdaCode, LambdaHandler.QUERY_LEAF_PARTITION, leafQueryFunctionName,
                "When a query arrives on the query SQS queue, this lambda is invoked to execute the query");

        attachPolicy(lambda, "LeafPartition");
        setPermissionsForLambda(coreStacks, jarsBucket, lambda, queryTrackingTable, leafPartitionQueryQueue, queryResultsQueue, queryResultsBucket);
        queryResultsQueue.grantConsumeMessages(coreStacks.getQueryPolicyForGrants());
        queryResultsBucket.grantReadWrite(coreStacks.getQueryPolicyForGrants());
        queryTrackingTable.grantReadData(coreStacks.getQueryPolicyForGrants());

        SqsEventSourceProps eventSourceProps = SqsEventSourceProps.builder()
                .batchSize(1)
                .build();

        lambda.addEventSource(new SqsEventSource(leafPartitionQueryQueue, eventSourceProps));

        return lambda;
    }

    /***
     * Attach a policy to allow the lambda to put results in any S3 bucket or on any SQS queue.
     * These policies look too open, but it's the only way to allow clients to be able to write
     * to their buckets and queues.
     *
     * @param lambda to apply the policy to
     */
    private void attachPolicy(IFunction lambda, String id) {
        PolicyStatementProps policyStatementProps = PolicyStatementProps.builder()
                .effect(Effect.ALLOW)
                .actions(Arrays.asList(S3Actions.PutObject.getActionName(), SQSActions.SendMessage.getActionName()))
                .resources(Collections.singletonList("*"))
                .build();
        PolicyStatement policyStatement = new PolicyStatement(policyStatementProps);
        String policyName = "PutToAnyS3BucketAndSendToAnySQSPolicy" + id;
        Policy policy = new Policy(this, policyName);
        policy.addStatements(policyStatement);
        Objects.requireNonNull(lambda.getRole()).attachInlinePolicy(policy);
    }

    /***
     * Creates the queue used for internal sub-queries against a specific leaf partition.
     *
     * @param  instanceProperties containing configuration details
     * @return                    the queue to be used for leaf partition queries
     */
    private Queue setupLeafPartitionQueryQueue(InstanceProperties instanceProperties, Topic topic, List<IMetric> errorMetrics) {
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String dlLeafPartitionQueueName = String.join("-", "sleeper", instanceId, "LeafPartitionQueryDLQ");
        Queue leafPartitionQueryDlq = Queue.Builder
                .create(this, "LeafPartitionQueryDeadLetterQueue")
                .queueName(dlLeafPartitionQueueName)
                .build();
        DeadLetterQueue leafPartitionQueryDeadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(leafPartitionQueryDlq)
                .build();
        String leafPartitionQueueName = String.join("-", "sleeper", instanceId, "LeafPartitionQueryQueue");
        Queue leafPartitionQueryQueue = Queue.Builder
                .create(this, "LeafPartitionQueryQueue")
                .queueName(leafPartitionQueueName)
                .deadLetterQueue(leafPartitionQueryDeadLetterQueue)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_URL, leafPartitionQueryQueue.getQueueUrl());
        instanceProperties.set(CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_ARN, leafPartitionQueryQueue.getQueueArn());
        instanceProperties.set(CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_DLQ_URL, leafPartitionQueryDlq.getQueueUrl());
        instanceProperties.set(CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_DLQ_ARN, leafPartitionQueryDlq.getQueueArn());
        createAlarmForDlq(this, "LeafPartitionQueryAlarm",
                "Alarms if there are any messages on the dead letter queue for the leaf partition query queue",
                leafPartitionQueryDlq, topic);
        errorMetrics.add(Utils.createErrorMetric("Subquery Errors", leafPartitionQueryDlq, instanceProperties));
        CfnOutputProps leafPartitionQueryQueueOutputNameProps = new CfnOutputProps.Builder()
                .value(leafPartitionQueryQueue.getQueueName())
                .exportName(instanceProperties.get(ID) + "-" + LEAF_PARTITION_QUERY_QUEUE_NAME)
                .build();
        new CfnOutput(this, LEAF_PARTITION_QUERY_QUEUE_NAME, leafPartitionQueryQueueOutputNameProps);

        CfnOutputProps leafPartitionQueryQueueOutputProps = new CfnOutputProps.Builder()
                .value(leafPartitionQueryQueue.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + LEAF_PARTITION_QUERY_QUEUE_URL)
                .build();
        new CfnOutput(this, LEAF_PARTITION_QUERY_QUEUE_URL, leafPartitionQueryQueueOutputProps);

        CfnOutputProps leafPartitionQueryDlqOutputProps = new CfnOutputProps.Builder()
                .value(leafPartitionQueryDlq.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + LEAF_PARTITION_QUERY_DLQ_URL)
                .build();
        new CfnOutput(this, LEAF_PARTITION_QUERY_DLQ_URL, leafPartitionQueryDlqOutputProps);

        return leafPartitionQueryQueue;
    }

    /***
     * Creates the queue used to publish results to.
     *
     * @param  instanceProperties containing configuration details
     * @return                    the queue created
     */
    private Queue setupResultsQueue(InstanceProperties instanceProperties) {
        String queueName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "QueryResultsQ");
        Queue resultsQueue = Queue.Builder
                .create(this, "QueryResultsQueue")
                .queueName(queueName)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_RESULTS_QUEUE_URL, resultsQueue.getQueueUrl());
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_RESULTS_QUEUE_ARN, resultsQueue.getQueueArn());

        CfnOutputProps resultsQueueOutputNameProps = new CfnOutputProps.Builder()
                .value(resultsQueue.getQueueName())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_RESULTS_QUEUE_NAME)
                .build();
        new CfnOutput(this, QUERY_RESULTS_QUEUE_NAME, resultsQueueOutputNameProps);

        CfnOutputProps resultsQueueOutputProps = new CfnOutputProps.Builder()
                .value(resultsQueue.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_RESULTS_QUEUE_URL)
                .build();
        new CfnOutput(this, QUERY_RESULTS_QUEUE_URL, resultsQueueOutputProps);

        return resultsQueue;
    }

    private IBucket setupResultsBucket(InstanceProperties instanceProperties, CoreStacks coreStacks, LambdaCode lambdaCode) {
        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);
        String bucketName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "query-results");
        Bucket resultsBucket = Bucket.Builder
                .create(this, "QueryResultsBucket")
                .bucketName(bucketName)
                .versioned(false)
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .encryption(BucketEncryption.S3_MANAGED)
                .removalPolicy(removalPolicy)
                .lifecycleRules(Collections.singletonList(
                        LifecycleRule.builder().expiration(Duration.days(instanceProperties.getInt(QUERY_RESULTS_BUCKET_EXPIRY_IN_DAYS))).build()))
                .build();
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET, resultsBucket.getBucketName());

        if (removalPolicy == RemovalPolicy.DESTROY) {
            AutoDeleteS3Objects.autoDeleteForBucket(this, instanceProperties, coreStacks, lambdaCode, resultsBucket, bucketName);
        }

        return resultsBucket;
    }

    /***
     * Sets the permissions a lambda needs to run queries. These are set for buckets and queues.
     *
     * @param coreStacks         the core stacks this belongs to
     * @param jarsBucket         bucket containing the jars used by Lambda
     * @param lambda             to apply the permissions to
     * @param queryTrackingTable used to track a query
     * @param queue              the queue to allow the Lambda to send messages to
     */
    private void setPermissionsForLambda(CoreStacks coreStacks, IBucket jarsBucket, IFunction lambda,
            ITable queryTrackingTable, IQueue queue) {
        coreStacks.grantReadTablesAndData(lambda);
        jarsBucket.grantRead(lambda);
        queue.grantSendMessages(lambda);
        queryTrackingTable.grantReadWriteData(lambda);

    }

    /***
     * Sets the permissions a lambda needs to run queries and write results.
     * These are set for buckets and queues.
     *
     * @param coreStacks         the core stacks this belongs to
     * @param jarsBucket         bucket containing the jars used by Lambda
     * @param lambda             to apply the permissions to
     * @param queryTrackingTable used to track a query
     * @param queue              the queue to allow the Lambda to send messages to
     * @param resultsQueue       the results queue to allow the Lambda to send result messages to
     * @param resultsBucket      the bucket that will contain the results
     */
    private void setPermissionsForLambda(CoreStacks coreStacks, IBucket jarsBucket, IFunction lambda,
            ITable queryTrackingTable, IQueue queue, IQueue resultsQueue, IBucket resultsBucket) {
        setPermissionsForLambda(coreStacks, jarsBucket, lambda, queryTrackingTable, queue);
        resultsBucket.grantReadWrite(lambda);
        resultsQueue.grantSendMessages(lambda);
    }

    public IFunction getQueryExecutorLambda() {
        return queryExecutorLambda;
    }

    public IFunction getLeafPartitionQueryLambda() {
        return leafPartitionQueryLambda;
    }
}
