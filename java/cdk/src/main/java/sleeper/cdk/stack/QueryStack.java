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
import software.amazon.awscdk.services.apigatewayv2.CfnApi;
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
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.IQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.cdk.jars.BuiltJar;
import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.configuration.properties.instance.CdkDefinedInstanceProperty;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Objects;

import static sleeper.cdk.Utils.createLambdaLogGroup;
import static sleeper.cdk.Utils.removalPolicy;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_TRACKER_TABLE_NAME;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.QueryProperty.QUERY_PROCESSOR_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.QueryProperty.QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.QueryProperty.QUERY_RESULTS_BUCKET_EXPIRY_IN_DAYS;

/**
 * A {@link NestedStack} to handle queries. This consists of a {@link Queue} that
 * queries are put on, a lambda {@link Function} to process them and another
 * {@link Queue} for the results to be posted to.
 */
@SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
public class QueryStack extends NestedStack {
    public static final String QUERY_QUEUE_NAME = "QueryQueueName";
    public static final String QUERY_QUEUE_URL = "QueryQueueUrl";
    public static final String QUERY_DL_QUEUE_URL = "QueryDLQueueUrl";
    public static final String LEAF_PARTITION_QUERY_QUEUE_NAME = "LeafPartitionQueryQueueName";
    public static final String LEAF_PARTITION_QUERY_QUEUE_URL = "LeafPartitionQueryQueueUrl";
    public static final String LEAF_PARTITION_QUERY_DL_QUEUE_URL = "LeafPartitionQueryDLQueueUrl";
    public static final String QUERY_RESULTS_QUEUE_NAME = "QueryResultsQueueName";
    public static final String QUERY_RESULTS_QUEUE_URL = "QueryResultsQueueUrl";
    public static final String QUERY_LAMBDA_ROLE_ARN = "QueryLambdaRoleArn";

    private CfnApi webSocketApi;
    private LambdaCode queryJar;
    private Queue queryQueriesQueue;
    private IFunction queryExecutorLambda;
    private IFunction leafPartitionQueryLambda;

    public QueryStack(Construct scope,
            String id,
            InstanceProperties instanceProperties,
            BuiltJars jars,
            CoreStacks coreStacks) {
        super(scope, id);

        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());
        String tableName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "query-tracking-table"));

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

        queryJar = jars.lambdaCode(BuiltJar.QUERY, jarsBucket);
        queryQueriesQueue = setupQueriesQueryQueue(instanceProperties);

        queryExecutorLambda = setupQueriesQueryLambda(coreStacks, instanceProperties, queryJar, queryQueriesQueue, jarsBucket, queryTrackingTable);
        leafPartitionQueryLambda = setupLeafPartitionQueryQueueAndLambda(coreStacks, instanceProperties, queryJar, jarsBucket, queryTrackingTable);
        Utils.addStackTagIfSet(this, instanceProperties);
    }

    /***
     * Creates a Lambda Function
     *
     * @param  id                 of the function to be created
     * @param  queryJar           the jar containing the code for the Lambda
     * @param  instanceProperties containing configuration details
     * @param  functionName       the name of the function
     * @param  handler            the path for the method be be used as the entry point for the Lambda
     * @param  description        a description for the function
     * @return                    an IFunction
     */
    private IFunction createFunction(String id, LambdaCode queryJar, InstanceProperties instanceProperties,
            String functionName, String handler, String description) {
        return queryJar.buildFunction(this, id, builder -> builder
                .functionName(functionName)
                .description(description)
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .memorySize(instanceProperties.getInt(QUERY_PROCESSOR_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS)))
                .handler(handler)
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .logGroup(createLambdaLogGroup(this, id + "LogGroup", functionName, instanceProperties)));
    }

    /***
     * Creates the Lambda needed for QueriesQuery
     *
     * @param  coreStacks         the core stacks this belongs to
     * @param  instanceProperties containing configuration details
     * @param  queryJar           the jar containing the code for the Lambda
     * @param  jarsBucket         bucket containing the jars used by Lambda
     * @param  queryTrackingTable used to track a query
     * @return                    the lambda created
     */
    private IFunction setupQueriesQueryLambda(CoreStacks coreStacks, InstanceProperties instanceProperties, LambdaCode queryJar,
            IQueue queryQueriesQueue, IBucket jarsBucket, ITable queryTrackingTable) {
        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "query-executor"));
        IFunction lambda = createFunction("QueryExecutorLambda", queryJar, instanceProperties, functionName,
                "sleeper.query.lambda.SqsQueryProcessorLambda::handleRequest",
                "When a query arrives on the query SQS queue, this lambda is invoked to look for leaf partition queries");

        attachPolicy(lambda, "Query");

        // Add the queue as a source of events for the lambdas
        SqsEventSourceProps eventSourceProps = SqsEventSourceProps.builder()
                .batchSize(1)
                .build();

        lambda.addEventSource(new SqsEventSource(queryQueriesQueue, eventSourceProps));

        setPermissionsForLambda(coreStacks, jarsBucket, lambda, queryTrackingTable, queryQueriesQueue);

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

    /***
     * Creates the queue and Lambda needed for LeafPartitionQuery.
     *
     * @param  coreStacks         the core stacks this belongs to
     * @param  instanceProperties containing configuration details
     * @param  queryJar           the jar containing the code for the Lambda
     * @param  jarsBucket         bucket containing the jars used by Lambda
     * @param  queryTrackingTable used to track a query
     * @return                    the lambda created
     */
    private IFunction setupLeafPartitionQueryQueueAndLambda(CoreStacks coreStacks, InstanceProperties instanceProperties, LambdaCode queryJar,
            IBucket jarsBucket, ITable queryTrackingTable) {
        Queue leafPartitionQueriesQueue = setupLeafPartitionQueryQueue(instanceProperties);
        Queue queryResultsQueue = setupResultsQueue(instanceProperties);
        IBucket queryResultsBucket = setupResultsBucket(instanceProperties);
        String leafQueryFunctionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "query-leaf-partition"));
        IFunction lambda = createFunction("QueryLeafPartitionExecutorLambda", queryJar, instanceProperties, leafQueryFunctionName,
                "sleeper.query.lambda.SqsLeafPartitionQueryLambda::handleRequest",
                "When a query arrives on the query SQS queue, this lambda is invoked to execute the query");

        attachPolicy(lambda, "LeafPartition");
        setPermissionsForLambda(coreStacks, jarsBucket, lambda, queryTrackingTable, leafPartitionQueriesQueue, queryResultsQueue, queryResultsBucket);

        SqsEventSourceProps eventSourceProps = SqsEventSourceProps.builder()
                .batchSize(1)
                .build();

        lambda.addEventSource(new SqsEventSource(leafPartitionQueriesQueue, eventSourceProps));

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
     * Creates the queue used for queries.
     *
     * @param  instanceProperties containing configuration details
     * @return                    the queue to be used for queries
     */
    private Queue setupQueriesQueryQueue(InstanceProperties instanceProperties) {
        String dlQueueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-QueryDLQ");
        Queue queryQueueForDLs = Queue.Builder
                .create(this, "QueriesDeadLetterQueue")
                .queueName(dlQueueName)
                .build();
        DeadLetterQueue queryDeadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(queryQueueForDLs)
                .build();
        String queryQueueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-QueriesQueue");
        Queue queryQueue = Queue.Builder
                .create(this, "QueriesQueue")
                .queueName(queryQueueName)
                .deadLetterQueue(queryDeadLetterQueue)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_QUEUE_URL, queryQueue.getQueueUrl());
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_QUEUE_ARN, queryQueue.getQueueArn());
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_DLQ_URL, queryQueueForDLs.getQueueUrl());
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_DLQ_ARN, queryQueueForDLs.getQueueArn());

        CfnOutputProps queriesQueueOutputNameProps = new CfnOutputProps.Builder()
                .value(queryQueue.getQueueName())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_QUEUE_NAME)
                .build();
        new CfnOutput(this, QUERY_QUEUE_NAME, queriesQueueOutputNameProps);

        CfnOutputProps queriesQueueOutputProps = new CfnOutputProps.Builder()
                .value(queryQueue.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_QUEUE_URL)
                .build();
        new CfnOutput(this, QUERY_QUEUE_URL, queriesQueueOutputProps);

        CfnOutputProps queriesDLQueueOutputProps = new CfnOutputProps.Builder()
                .value(queryQueueForDLs.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_DL_QUEUE_URL)
                .build();
        new CfnOutput(this, QUERY_DL_QUEUE_URL, queriesDLQueueOutputProps);

        return queryQueue;
    }

    /***
     * Creates the queue used for leaf partition queries
     *
     * @param  instanceProperties containing configuration details
     * @return                    the queue to be used for leaf partition queries
     */
    private Queue setupLeafPartitionQueryQueue(InstanceProperties instanceProperties) {
        String dlLeafPartitionQueueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-LeafPartitionQueryDLQ");
        Queue leafPartitionQueryQueueForDLs = Queue.Builder
                .create(this, "LeafPartitionQueriesDeadLetterQueue")
                .queueName(dlLeafPartitionQueueName)
                .build();
        DeadLetterQueue leafPartitionQueriesDeadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(leafPartitionQueryQueueForDLs)
                .build();
        String leafPartitionQueueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-LeafPartitionQueriesQueue");
        Queue leafPartitionQueriesQueue = Queue.Builder
                .create(this, "LeafPartitionQueriesQueue")
                .queueName(leafPartitionQueueName)
                .deadLetterQueue(leafPartitionQueriesDeadLetterQueue)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_URL, leafPartitionQueriesQueue.getQueueUrl());
        instanceProperties.set(CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_ARN, leafPartitionQueriesQueue.getQueueArn());
        instanceProperties.set(CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_DLQ_URL, leafPartitionQueryQueueForDLs.getQueueUrl());
        instanceProperties.set(CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_DLQ_ARN, leafPartitionQueryQueueForDLs.getQueueArn());

        CfnOutputProps leafPartitionQueriesQueueOutputNameProps = new CfnOutputProps.Builder()
                .value(leafPartitionQueriesQueue.getQueueName())
                .exportName(instanceProperties.get(ID) + "-" + LEAF_PARTITION_QUERY_QUEUE_NAME)
                .build();
        new CfnOutput(this, LEAF_PARTITION_QUERY_QUEUE_NAME, leafPartitionQueriesQueueOutputNameProps);

        CfnOutputProps leafPartitionQueriesQueueOutputProps = new CfnOutputProps.Builder()
                .value(leafPartitionQueriesQueue.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + LEAF_PARTITION_QUERY_QUEUE_URL)
                .build();
        new CfnOutput(this, LEAF_PARTITION_QUERY_QUEUE_URL, leafPartitionQueriesQueueOutputProps);

        CfnOutputProps leafPartitionQueriesDLQueueOutputProps = new CfnOutputProps.Builder()
                .value(leafPartitionQueryQueueForDLs.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + LEAF_PARTITION_QUERY_DL_QUEUE_URL)
                .build();
        new CfnOutput(this, LEAF_PARTITION_QUERY_DL_QUEUE_URL, leafPartitionQueriesDLQueueOutputProps);

        return leafPartitionQueriesQueue;
    }

    /***
     * Creates the queue used to publish results to.
     *
     * @param  instanceProperties containing configuration details
     * @return                    the queue created
     */
    private Queue setupResultsQueue(InstanceProperties instanceProperties) {
        String queueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-QueryResultsQ");
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

    /***
     * Creates the bucket used to store results.
     *
     * @param  instanceProperties containing configuration details
     * @return                    the bucket created
     */
    private IBucket setupResultsBucket(InstanceProperties instanceProperties) {
        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);
        Bucket resultsBucket = Bucket.Builder
                .create(this, "QueryResultsBucket")
                .bucketName(String.join("-", "sleeper", instanceProperties.get(ID), "query-results").toLowerCase(Locale.ROOT))
                .versioned(false)
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .encryption(BucketEncryption.S3_MANAGED)
                .removalPolicy(removalPolicy).autoDeleteObjects(removalPolicy == RemovalPolicy.DESTROY)
                .lifecycleRules(Collections.singletonList(
                        LifecycleRule.builder().expiration(Duration.days(instanceProperties.getInt(QUERY_RESULTS_BUCKET_EXPIRY_IN_DAYS))).build()))
                .build();
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET, resultsBucket.getBucketName());

        return resultsBucket;
    }

    /***
     * Sets the permissions a Lambda needs to execute. These are set for buckets and queues.
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
     * Sets the permissions a Lambda needs to execute along with a place to write results to.
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

    public LambdaCode getQueryJar() {
        return queryJar;
    }

    public Queue getQueryQueue() {
        return queryQueriesQueue;
    }

    public IFunction getQueryExecutorLambda() {
        return queryExecutorLambda;
    }

    public IFunction getLeafPartitionQueryLambda() {
        return leafPartitionQueryLambda;
    }

}
