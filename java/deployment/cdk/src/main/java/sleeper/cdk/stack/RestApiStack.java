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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.aws_apigatewayv2_authorizers.HttpIamAuthorizer;
import software.amazon.awscdk.aws_apigatewayv2_integrations.HttpLambdaIntegration;
import software.amazon.awscdk.services.apigatewayv2.AddRoutesOptions;
import software.amazon.awscdk.services.apigatewayv2.HttpApi;
import software.amazon.awscdk.services.apigatewayv2.HttpMethod;
import software.amazon.awscdk.services.lambda.IFunction;
import software.constructs.Construct;

import sleeper.cdk.artefacts.SleeperInstanceArtefacts;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.List;
import java.util.Map;

/**
 * REST API for interacting with the Sleeper instance.
 * Utilises API Gateway.
 */
@SuppressFBWarnings("MC_OVERRIDABLE_METHOD_CALL_IN_CONSTRUCTOR")
public class RestApiStack extends NestedStack {

    public RestApiStack(Construct scope, String id, InstanceProperties instanceProperties,
            SleeperInstanceArtefacts artefacts, SleeperCoreStacks coreStacks) {
        super(scope, id);
        String instanceId = instanceProperties.cleanInstanceId();
        SleeperLambdaCode lambdaCode = artefacts.lambdaCodeAtScope(this);
        Map<String, String> env = EnvironmentUtils.createDefaultEnvironment(instanceProperties);
        String functionName = String.join("-", "sleeper", instanceId, "rest-api-handler");
        IFunction lambda = lambdaCode.buildFunction(LambdaHandler.REST_API_HANDLER, "RestApiHandlerlambda", builder -> builder
                .functionName(functionName)
                .description("Implements a REST API for interacting with Sleeper")
                .environment(env)
                .memorySize(1024)
                .logGroup(coreStacks.getLogGroup(LogGroupRef.REST_API_HANDLER))
                .timeout(Duration.seconds(29)));

        HttpApi restHttpApi = HttpApi.Builder.create(this, "RestApi")
                .description("Sleeper REST API")
                .apiName(lambda.getFunctionName())
                .defaultAuthorizer(new HttpIamAuthorizer())
                .build();

        HttpLambdaIntegration integration = HttpLambdaIntegration.Builder.create(instanceId, lambda).build();
        restHttpApi.addRoutes(AddRoutesOptions.builder()
                .path("/sleeper")
                .methods(List.of(HttpMethod.GET))
                .integration(integration).build());
        restHttpApi.addRoutes(AddRoutesOptions.builder()
                .path("/sleeper/tables")
                .methods(List.of(HttpMethod.POST))
                .integration(integration).build());

        new CfnOutput(this, "RestApiUrl", CfnOutputProps.builder()
                .value(restHttpApi.getApiEndpoint())
                .build());
        instanceProperties.set(CdkDefinedInstanceProperty.REST_API_URL, restHttpApi.getApiEndpoint());
    }
}
