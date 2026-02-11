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
package sleeper.cdk.stack.query;

import software.amazon.awscdk.ArnComponents;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.services.apigateway.IntegrationType;
import software.amazon.awscdk.services.apigatewayv2.CfnApi;
import software.amazon.awscdk.services.apigatewayv2.CfnIntegration;
import software.amazon.awscdk.services.apigatewayv2.CfnRoute;
import software.amazon.awscdk.services.apigatewayv2.WebSocketApi;
import software.amazon.awscdk.services.apigatewayv2.WebSocketApiAttributes;
import software.amazon.awscdk.services.apigatewayv2.WebSocketStage;
import software.amazon.awscdk.services.iam.Grant;
import software.amazon.awscdk.services.iam.GrantOnPrincipalOptions;
import software.amazon.awscdk.services.iam.IGrantable;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.Permission;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.artefacts.SleeperJarsInBucket;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.List;
import java.util.Map;

public final class WebSocketQueryStack extends NestedStack {

    private CfnApi webSocketApi;

    public WebSocketQueryStack(Construct scope,
            String id,
            InstanceProperties instanceProperties,
            SleeperJarsInBucket jars, SleeperCoreStacks coreStacks, QueryQueueStack queryQueueStack, QueryStack queryStack) {
        super(scope, id);

        IBucket jarsBucket = jars.createJarsBucketReference(this, "JarsBucket");
        SleeperLambdaCode lambdaCode = jars.lambdaCode(jarsBucket);
        setupWebSocketApi(instanceProperties, lambdaCode, coreStacks, queryQueueStack, queryStack);
        Utils.addTags(this, instanceProperties);
    }

    private void setupWebSocketApi(InstanceProperties instanceProperties, SleeperLambdaCode lambdaCode,
            SleeperCoreStacks coreStacks, QueryQueueStack queryQueueStack, QueryStack queryStack) {
        Map<String, String> env = EnvironmentUtils.createDefaultEnvironment(instanceProperties);
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String functionName = String.join("-", "sleeper", instanceId, "query-websocket-handler");
        IFunction webSocketApiHandler = lambdaCode.buildFunction(this, LambdaHandler.WEB_SOCKET_QUERY, "WebSocketApiHandler", builder -> builder
                .functionName(functionName)
                .description("Prepares queries received via the WebSocket API and queues them for processing")
                .environment(env)
                .memorySize(1024)
                .logGroup(coreStacks.getLogGroup(LogGroupRef.QUERY_WEBSOCKET_HANDLER))
                .timeout(Duration.seconds(29)));

        queryQueueStack.grantSendMessages(webSocketApiHandler);
        coreStacks.grantReadTablesConfig(webSocketApiHandler);

        CfnApi api = CfnApi.Builder.create(this, "api")
                .name(String.join("-", "sleeper", instanceId, "query-websocket-api"))
                .description("Sleeper Query API")
                .protocolType("WEBSOCKET")
                .routeSelectionExpression("$request.body.action")
                .build();
        this.webSocketApi = api;

        String integrationUri = Stack.of(this).formatArn(ArnComponents.builder()
                .service("apigateway")
                .account("lambda")
                .resource("path/2015-03-31/functions")
                .resourceName(webSocketApiHandler.getFunctionArn() + "/invocations")
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

        webSocketApiHandler.addPermission("apigateway-access-to-lambda", Permission.builder()
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
        stage.grantManagementApiAccess(webSocketApiHandler);
        stage.grantManagementApiAccess(queryStack.getQueryExecutorLambda());
        stage.grantManagementApiAccess(queryStack.getLeafPartitionQueryLambda());
        grantAccessToWebSocketQueryApi(coreStacks.getQueryPolicyForGrants());

        new CfnOutput(this, "WebSocketApiUrl", CfnOutputProps.builder()
                .value(stage.getUrl())
                .build());
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL, stage.getUrl());
    }

    /***
     * Grant access to the web socket query api.
     *
     * @param  identity item to grant access to
     * @return          a grant principal
     */
    public Grant grantAccessToWebSocketQueryApi(IGrantable identity) {
        return Grant.addToPrincipal(GrantOnPrincipalOptions.builder()
                .grantee(identity)
                .actions(List.of("execute-api:Invoke"))
                .resourceArns(List.of(Stack.of(this).formatArn(ArnComponents.builder()
                        .service("execute-api")
                        .resource(this.webSocketApi.getRef())
                        .build())
                        + "/live/*"))
                .build());
    }
}
