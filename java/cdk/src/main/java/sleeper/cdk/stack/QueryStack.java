/*
 * Copyright 2022 Crown Copyright
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

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_TRACKER_TABLE_NAME;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.QUERY_PROCESSOR_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.QUERY_RESULTS_BUCKET_EXPIRY_IN_DAYS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.RETAIN_INFRA_AFTER_DESTROY;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VERSION;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.auth.policy.actions.SQSActions;

import sleeper.cdk.Utils;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SystemDefinedInstanceProperty;
import software.amazon.awscdk.ArnComponents;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.services.apigateway.IntegrationType;
import software.amazon.awscdk.services.apigatewayv2.CfnApi;
import software.amazon.awscdk.services.apigatewayv2.CfnIntegration;
import software.amazon.awscdk.services.apigatewayv2.CfnRoute;
import software.amazon.awscdk.services.apigatewayv2.alpha.WebSocketApi;
import software.amazon.awscdk.services.apigatewayv2.alpha.WebSocketApiAttributes;
import software.amazon.awscdk.services.apigatewayv2.alpha.WebSocketStage;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.Grant;
import software.amazon.awscdk.services.iam.GrantOnPrincipalOptions;
import software.amazon.awscdk.services.iam.IGrantable;
import software.amazon.awscdk.services.iam.Policy;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.PolicyStatementProps;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.Permission;
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

/**
 * A {@link Stack} to handle queries. This consists of a {@link Queue} that
 * queries are put on, a lambda {@link Function} to process them and another
 * {@link Queue} for the results to be posted to.
 */
public class QueryStack extends NestedStack {
    public static final String QUERY_QUEUE_NAME = "QueryQueueName";
    public static final String QUERY_QUEUE_URL = "QueryQueueUrl";
    public static final String QUERY_DL_QUEUE_URL = "QueryDLQueueUrl";
    public static final String QUERY_RESULTS_QUEUE_NAME = "QueryResultsQueueName";
    public static final String QUERY_RESULTS_QUEUE_URL = "QueryResultsQueueUrl";
    public static final String QUERY_LAMBDA_ROLE_ARN = "QueryLambdaRoleArn";

    private CfnApi webSocketApi;

    public QueryStack(Construct scope,
                      String id,
                      List<IBucket> dataBuckets,
                      List<StateStoreStack> stateStoreStacks,
                      InstanceProperties instanceProperties) {
        super(scope, id);

        // Config bucket
        IBucket configBucket = Bucket.fromBucketName(this, "ConfigBucket", instanceProperties.get(CONFIG_BUCKET));

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", instanceProperties.get(JARS_BUCKET));

        String tableName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(), "query-tracking-table"));

        // Query Tracking Table
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

        // Queue for queries
        String dlQueueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-QueryDLQ");
        Queue queueForDLs = Queue.Builder
                .create(this, "QueriesDeadLetterQueue")
                .queueName(dlQueueName)
                .build();
        DeadLetterQueue queriesDeadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(queueForDLs)
                .build();
        String queueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-QueriesQueue");
        Queue queriesQueue = Queue.Builder
                .create(this, "QueriesQueue")
                .queueName(queueName)
                .deadLetterQueue(queriesDeadLetterQueue)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(SystemDefinedInstanceProperty.QUERY_QUEUE_URL, queriesQueue.getQueueUrl());
        instanceProperties.set(SystemDefinedInstanceProperty.QUERY_DLQ_URL, queueForDLs.getQueueUrl());

        // Queue for results
        String queryResultsQueueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-QueryResultsQ");
        Queue queryResultsQueue = Queue.Builder
                .create(this, "QueryResultsQueue")
                .queueName(queryResultsQueueName)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(SystemDefinedInstanceProperty.QUERY_RESULTS_QUEUE_URL, queryResultsQueue.getQueueUrl());

        RemovalPolicy removalPolicy;
        if (instanceProperties.getBoolean(RETAIN_INFRA_AFTER_DESTROY)) {
            removalPolicy = RemovalPolicy.RETAIN;
        } else {
            removalPolicy = RemovalPolicy.DESTROY;
        }

        // Bucket for results
        Bucket queryResultsBucket = Bucket.Builder
                .create(this, "QueryResultsBucket")
                .bucketName(String.join("-", "sleeper", instanceProperties.get(ID), "query-results").toLowerCase())
                .versioned(false)
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .encryption(BucketEncryption.S3_MANAGED)
                .removalPolicy(removalPolicy)
                .lifecycleRules(Collections.singletonList(
                        LifecycleRule.builder().expiration(Duration.days(instanceProperties.getInt(QUERY_RESULTS_BUCKET_EXPIRY_IN_DAYS))).build()))
                .build();
        instanceProperties.set(SystemDefinedInstanceProperty.QUERY_RESULTS_BUCKET, queryResultsBucket.getBucketName());

        CfnOutputProps queriesQueueOutputNameProps = new CfnOutputProps.Builder()
                .value(queriesQueue.getQueueName())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_QUEUE_NAME)
                .build();
        new CfnOutput(this, QUERY_QUEUE_NAME, queriesQueueOutputNameProps);

        CfnOutputProps queriesQueueOutputProps = new CfnOutputProps.Builder()
                .value(queriesQueue.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_QUEUE_URL)
                .build();
        new CfnOutput(this, QUERY_QUEUE_URL, queriesQueueOutputProps);

        CfnOutputProps queriesDLQueueOutputProps = new CfnOutputProps.Builder()
                .value(queueForDLs.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_DL_QUEUE_URL)
                .build();
        new CfnOutput(this, QUERY_DL_QUEUE_URL, queriesDLQueueOutputProps);

        CfnOutputProps queryResultsQueueOutputNameProps = new CfnOutputProps.Builder()
                .value(queryResultsQueue.getQueueName())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_RESULTS_QUEUE_NAME)
                .build();
        new CfnOutput(this, QUERY_RESULTS_QUEUE_NAME, queryResultsQueueOutputNameProps);

        CfnOutputProps queryResultsQueueOutputProps = new CfnOutputProps.Builder()
                .value(queryResultsQueue.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_RESULTS_QUEUE_URL)
                .build();
        new CfnOutput(this, QUERY_RESULTS_QUEUE_URL, queryResultsQueueOutputProps);

        // Query execution lambda code
        Code code = Code.fromBucket(jarsBucket, "query-" + instanceProperties.get(VERSION) + ".jar");

        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(), "query-executor"));

        // Lambda to process queries and post results to results queue
        Function queryExecutorLambda = Function.Builder
                .create(this, "QueryExecutorLambda")
                .functionName(functionName)
                .description("When a query arrives on the query SQS queue, this lambda is invoked to perform the query")
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_8)
                .memorySize(instanceProperties.getInt(QUERY_PROCESSOR_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS)))
                .code(code)
                .handler("sleeper.query.lambda.SqsQueryProcessorLambda::handleRequest")
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .build();

        // Add the queue as a source of events for this lambda
        SqsEventSourceProps eventSourceProps = SqsEventSourceProps.builder()
                .batchSize(1)
                .build();
        queryExecutorLambda.addEventSource(new SqsEventSource(queriesQueue, eventSourceProps));

        // Grant the lambda permission to read from the Dynamo tables, read from
        // the S3 bucket, write back to the query queue and write to the results
        // queue and S3 bucket
        dataBuckets.forEach(bucket -> bucket.grantRead(queryExecutorLambda));
        stateStoreStacks.forEach(stateStoreStack -> stateStoreStack.grantReadActiveFileMetadata(queryExecutorLambda));
        stateStoreStacks.forEach(stateStoreStack -> stateStoreStack.grantReadPartitionMetadata(queryExecutorLambda));
        configBucket.grantRead(queryExecutorLambda);
        jarsBucket.grantRead(queryExecutorLambda);
        queriesQueue.grantSendMessages(queryExecutorLambda);
        queryResultsQueue.grantSendMessages(queryExecutorLambda);
        queryResultsBucket.grantReadWrite(queryExecutorLambda);
        queryTrackingTable.grantReadWriteData(queryExecutorLambda);

        // Attach a policy to allow the lambda to put results in any S3 bucket or on any SQS queue.
        // These policies look too open, but it's the only way to allow clients to be able to write to their
        // buckets and queues.
        PolicyStatementProps policyStatementProps = PolicyStatementProps.builder()
                .effect(Effect.ALLOW)
                .actions(Arrays.asList(S3Actions.PutObject.getActionName(), SQSActions.SendMessage.getActionName()))
                .resources(Collections.singletonList("*"))
                .build();
        PolicyStatement policyStatement = new PolicyStatement(policyStatementProps);
        Policy policy = new Policy(this, "PutToAnyS3BucketAndSendToAnySQSPolicy");
        policy.addStatements(policyStatement);
        queryExecutorLambda.getRole().attachInlinePolicy(policy);

        // Output the role of the lambda as a property so that clients that want the results of queries written
        // to their own SQS queue can give the role permission to write to their queue
        CfnOutputProps queryLambdaRoleOutputProps = new CfnOutputProps.Builder()
                .value(queryExecutorLambda.getRole().getRoleArn())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_LAMBDA_ROLE_ARN)
                .build();
        new CfnOutput(this, QUERY_LAMBDA_ROLE_ARN, queryLambdaRoleOutputProps);
        instanceProperties.set(SystemDefinedInstanceProperty.QUERY_LAMBDA_ROLE, queryExecutorLambda.getRole().getRoleName());

        this.setupWebSocketApi(code, instanceProperties, queriesQueue, queryExecutorLambda, configBucket);
    }

    protected void setupWebSocketApi(Code queryCode, InstanceProperties instanceProperties, Queue queriesQueue, Function queryExecutorLambda, IBucket configBucket) {
        Map<String, String> env = Utils.createDefaultEnvironment(instanceProperties);
        Function handler = Function.Builder.create(this, "apiHandler")
                .description("Prepares queries received via the WebSocket API and queues them for processing")
                .code(queryCode)
                .handler("sleeper.query.lambda.WebSocketQueryProcessorLambda::handleRequest")
                .environment(env)
                .memorySize(256)
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .timeout(Duration.seconds(29))
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_8)
                .build();
        queriesQueue.grantSendMessages(handler);
        configBucket.grantRead(handler);

        CfnApi api = CfnApi.Builder.create(this, "api")
                .name("sleeper-" + instanceProperties.get(ID) + "-query-api")
                .description("Sleeper Query API")
                .protocolType("WEBSOCKET")
                .routeSelectionExpression("$request.body.action")
                .build();
        this.webSocketApi = api;

        String integrationUri = Stack.of(this).formatArn(ArnComponents.builder()
                .service("apigateway")
                .account("lambda")
                .resource("path/2015-03-31/functions")
                .resourceName(handler.getFunctionArn() + "/invocations")
                .build());

        CfnIntegration integration = CfnIntegration.Builder.create(this, "integration")
                .apiId(api.getRef())
                .integrationType(IntegrationType.AWS_PROXY.name())
                .integrationUri(integrationUri)
                .build();

        // Note that we are deliberately using CFN L1 constructs to deploy the connect
        // route so that we are able to switch on AWS_IAM authentication. This is
        // currently not possible using the API Gateway L2 constructs
        CfnRoute.Builder.create(this, "connect-route")
                .apiId(api.getRef())
                .apiKeyRequired(false)
                .authorizationType("AWS_IAM")
                .routeKey("$connect")
                .target("integrations/" + integration.getRef())
                .build();

        CfnRoute.Builder.create(this, "default-route")
                .apiId(api.getRef())
                .apiKeyRequired(false)
                .routeKey("$default")
                .target("integrations/" + integration.getRef())
                .build();

        handler.addPermission("apigateway-access-to-lambda", Permission.builder()
                .principal(new ServicePrincipal("apigateway.amazonaws.com"))
                .sourceArn(Stack.of(this).formatArn(ArnComponents.builder()
                        .service("execute-api")
                        .resource(api.getRef())
                        .resourceName("*/*")
                        .build()))
                .build());

        WebSocketStage stage = WebSocketStage.Builder.create(this, "stage")
                .webSocketApi(WebSocketApi.fromWebSocketApiAttributes(this, "imported-api", WebSocketApiAttributes.builder()
                        .webSocketId(api.getRef())
                        .build()))
                .stageName("live")
                .autoDeploy(true)
                .build();
        stage.grantManagementApiAccess(handler);
        stage.grantManagementApiAccess(queryExecutorLambda);

        new CfnOutput(this, "WebSocketApiUrl", CfnOutputProps.builder()
                .value(stage.getUrl())
                .build());
        instanceProperties.set(SystemDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL, stage.getUrl());
    }

    public Grant grantAccessToWebSocketQueryApi(IGrantable identity) {
        return Grant.addToPrincipal(GrantOnPrincipalOptions.builder()
                .grantee(identity)
                .actions(Collections.singletonList("execute-api:Invoke"))
                .resourceArns(Collections.singletonList(Stack.of(this).formatArn(ArnComponents.builder()
                        .service("execute-api")
                        .resource(this.webSocketApi.getRef())
                        .build())
                        + "/live/*"
                        ))
                .build());
    }
}
