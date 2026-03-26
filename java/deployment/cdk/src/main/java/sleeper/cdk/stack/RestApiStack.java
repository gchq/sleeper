/*
 * Copyright 2022-2026 Crown Copyright
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

import software.amazon.awscdk.ArnComponents;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.services.apigatewayv2.CfnIntegration;
import software.amazon.awscdk.services.apigatewayv2.CfnRoute;
import software.amazon.awscdk.services.apigatewayv2.HttpApi;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.Permission;
import software.amazon.awscdk.services.logs.ILogGroup;
import software.constructs.Construct;

import sleeper.cdk.artefacts.SleeperInstanceArtefacts;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.cdk.stack.core.LoggingStack;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.Map;

import static sleeper.core.properties.instance.CommonProperty.ID;

/**
 * Rest api for interacting with the sleeper instance.
 * Utilises API Gateway.
 *
 */
public class RestApiStack extends NestedStack {

    public RestApiStack(Construct scope, String id, InstanceProperties instanceProperties,
            SleeperInstanceArtefacts artefacts) {
        super(scope, id);
        ILogGroup logGroup = LoggingStack.createLogGroup(this, LogGroupRef.REST_API_HANDLER, instanceProperties);
        setUpRestApi(scope, instanceProperties, id, artefacts, logGroup);
    }

    private void setUpRestApi(Construct scope, InstanceProperties instanceProperties, String constructId,
            SleeperInstanceArtefacts artefacts, ILogGroup logGroup) {
        String instanceId = Utils.cleanInstanceId(instanceProperties.get(ID));
        SleeperLambdaCode lambdaCode = artefacts.lambdaCodeAtScope(this);
        Map<String, String> env = EnvironmentUtils.createDefaultEnvironment(instanceProperties);
        String functionName = String.join("-", "sleeper", instanceId, "rest-api-handler");
        IFunction lambda = lambdaCode.buildFunction(LambdaHandler.REST_API_HANDLER, constructId + "-lambda", builder -> builder
                .functionName(functionName)
                .description("Implements a REST API for interacting with SLEEPER")
                .environment(env)
                .memorySize(1024)
                .logGroup(logGroup)
                .timeout(Duration.seconds(29)));

        String restApiId = "sleeper-restapi-" + instanceId;
        String restApiUri = setupRestApiUri(this, lambda);

        HttpApi restHttpApi = setupApiGateway(this, restApiId);

        CfnIntegration restApiIntegration = CfnIntegration.Builder.create(this, "restapi-integration")
                .apiId(restHttpApi.getApiId())
                .integrationType("AWS_PROXY")
                .integrationUri(restApiUri)
                .build();

        CfnRoute.Builder.create(this, "connect-route")
                .apiId(restHttpApi.getApiId())
                .apiKeyRequired(false)
                .authorizationType("AWS_IAM")
                .routeKey("$connect")
                .target("integrations/" + restApiIntegration.getRef())
                .build();

        lambda.addPermission("apigateway-access-to-lambda", Permission.builder()
                .principal(new ServicePrincipal("apigateway.amazonaws.com"))
                .sourceArn(Stack.of(this).formatArn(ArnComponents.builder()
                        .service("execute-api")
                        .resource(restHttpApi.getApiId())
                        .resourceName("*/*")
                        .build()))
                .build());

        new CfnOutput(this, "RestApiUrl", CfnOutputProps.builder()
                .value(restHttpApi.getApiEndpoint())
                .build());
        instanceProperties.set(CdkDefinedInstanceProperty.REST_API_URL, restHttpApi.getApiEndpoint());
    }

    private String setupRestApiUri(Construct scope, IFunction lambda) {
        return Stack.of(scope).formatArn(ArnComponents.builder()
                .service("apigateway")
                .account("lambda")
                .resource("path/2015-03-31/functions")
                .resourceName(lambda.getFunctionArn() + "/invocations")
                .build());
    }

    private HttpApi setupApiGateway(Construct scope, String id) {
        return HttpApi.Builder.create(scope, id)
                .description("Sleeper Rest Api")
                .apiName("sleeper-rest-api")
                .build();
    }
}
