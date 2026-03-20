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
import software.amazon.awscdk.services.apigateway.CfnRestApi;
import software.amazon.awscdk.services.apigateway.IntegrationType;
import software.amazon.awscdk.services.apigatewayv2.CfnIntegration;
import software.amazon.awscdk.services.apigatewayv2.CfnRoute;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.Permission;
import software.amazon.awssdk.services.apigateway.model.CreateRestApiRequest;
import software.constructs.Construct;

import sleeper.cdk.artefacts.SleeperInstanceArtefacts;
import sleeper.cdk.lambda.SleeperLambdaCode;
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
        setUpRestApi(scope, instanceProperties, id, artefacts);
    }

    private void setUpRestApi(Construct scope, InstanceProperties instanceProperties, String constructId, SleeperInstanceArtefacts artefacts) {
        String instanceId = Utils.cleanInstanceId(instanceProperties.get(ID));
        SleeperLambdaCode lambdaCode = artefacts.lambdaCodeAtScope(this);
        Map<String, String> env = EnvironmentUtils.createDefaultEnvironment(instanceProperties);
        String functionName = String.join("-", "sleeper", instanceId, "rest-api-handler");
        IFunction lambda = lambdaCode.buildFunction(LambdaHandler.REST_API_HANDLER, constructId, builder -> builder
                .functionName(functionName)
                .description("Function for creating REST API for interacting with SLEEPER")
                .environment(env)
                .memorySize(1024)
                // Need a log group
                .timeout(Duration.seconds(29)));

        String restApiId = "sleeper-restapi-" + instanceId;
        String restApiUri = setupRestApiUri(scope, lambda);

        CfnRestApi restApi = setupApiGateway(scope, restApiId);

        CfnIntegration restApiIntegration = CfnIntegration.Builder.create(scope, "restapi-integration")
                .apiId(restApi.getRef())
                .integrationType(IntegrationType.AWS_PROXY.name())
                .integrationUri(restApiUri)
                .build();

        CfnRoute.Builder.create(scope, "connect-route")
                .apiId(restApi.getRef())
                .apiKeyRequired(false)
                .authorizationType("AWS_IAM")
                .routeKey("$connect")
                .target("integrations/" + restApiIntegration.getRef())
                .build();

        lambda.addPermission("apigateway-access-to-lambda", Permission.builder()
                .principal(new ServicePrincipal("apigateway.amazonaws.com"))
                .sourceArn(Stack.of(scope).formatArn(ArnComponents.builder()
                        .service("execute-api")
                        .resource(restApi.getRef())
                        .resourceName("*/*")
                        .build()))
                .build());

        String restApiUrl = buildRestApiUrl(restApi);

        new CfnOutput(this, "RestApiUrl", CfnOutputProps.builder()
                .value(restApiUrl)
                .build());
        instanceProperties.set(CdkDefinedInstanceProperty.REST_API_URL, restApiUrl);
    }

    private String setupRestApiUri(Construct scope, IFunction lambda) {
        return Stack.of(scope).formatArn(ArnComponents.builder()
                .service("apigateway")
                .account("lambda")
                .resource("path/2015-03-31/functions")
                .resourceName(lambda.getFunctionArn() + "/invocations")
                .build());
    }

    private CfnRestApi setupApiGateway(Construct scope, String id) {
        return CfnRestApi.Builder.create(scope, id)
                .body(createApiBody())
                // Exists alternate of having the restApi yaml stored within an S3 Bucket [BodyS3Location]
                .description("Sleeper Rest Api")
                .name("sleeper-rest-api")
                .build();
    }

    private String buildRestApiUrl(CfnRestApi restApi) {
        // TODO Needs elobrating
        return "";
    }

    // TODO: Method to add all calls to the rest API (YAML Format)
    private Object createApiBody() {
        return createDummyRestApiRequest("test-id", "getVersion");
    }

    // TODO: Remove/replace this method, added to provide placeholder
    private CreateRestApiRequest createDummyRestApiRequest(String id, String name) {
        return CreateRestApiRequest.builder()
                .cloneFrom(id)
                .description("Get version of sleeper")
                .name(name)
                .build();
    }
}
