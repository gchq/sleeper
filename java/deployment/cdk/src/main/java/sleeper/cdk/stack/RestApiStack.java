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
import software.amazon.awscdk.CfnTag;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.services.apigateway.CfnRestApi;
import software.amazon.awscdk.services.apigateway.CfnRestApi.EndpointConfigurationProperty;
import software.amazon.awscdk.services.apigateway.CfnRestApi.S3LocationProperty;
import software.amazon.awscdk.services.apigateway.IntegrationType;
import software.amazon.awscdk.services.apigatewayv2.CfnIntegration;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awssdk.services.apigateway.model.CreateRestApiRequest;
import software.constructs.Construct;

import sleeper.cdk.artefacts.SleeperInstanceArtefacts;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.List;
import java.util.Map;

import static sleeper.core.properties.instance.CommonProperty.ID;

/**
 * Rest api for interacting with the sleeper instance.
 * Utilises API Gateway.
 *
 * TODO Futher expand this javadoc
 */
public class RestApiStack extends NestedStack {

    public RestApiStack(Construct scope, String id, InstanceProperties instanceProperties,
            SleeperInstanceArtefacts artefacts) {
        super(scope, instanceProperties.get(ID));
        SleeperLambdaCode lambdaCode = artefacts.lambdaCodeAtScope(this);
        setUpRestApi(scope, instanceProperties, instanceProperties.get(ID), lambdaCode);
    }

    private void setUpRestApi(Construct scope, InstanceProperties instanceProperties, String id, SleeperLambdaCode lambdaCode) {
        //This section to elobrate/set from elsewhere
        //String restApiName = "rest-api-name";

        Map<String, String> env = EnvironmentUtils.createDefaultEnvironment(instanceProperties);
        String functionName = String.join("-", "sleeper", id, "rest-api-handler");
        IFunction lambdaFunction = lambdaCode.buildFunction(LambdaHandler.REST_API_HANDLER, id, builder -> builder
                .functionName(functionName)
                .description("Function for creating rest api for interacting with sleeper")
                .environment(env)
                .memorySize(1024)
                // Need a log group
                .timeout(Duration.seconds(29)));

        String restApiUri = Stack.of(this).formatArn(ArnComponents.builder()
                .service("apigateway")
                .account("lambda")
                .resource("path/2015-03-31/functions")
                .resourceName(lambdaFunction.getFunctionArn() + "/invocations")
                .build());

        CfnRestApi restApi = setUpApiGateway(scope, instanceProperties.get(ID));
        CfnIntegration restApiIntegration = CfnIntegration.Builder.create(this, "integration")
                .apiId(restApi.getRef())
                .integrationType(IntegrationType.AWS_PROXY.name())
                .integrationUri(restApiUri)
                .build();

    }

    // TODO Check and re-enable properties
    private CfnRestApi setUpApiGateway(Construct scope, String instanceId) {
        return CfnRestApi.Builder.create(this, instanceId)
                //.apiKeySourceType("apiKeySourceType")
                //.binaryMediaTypes(List.of("binaryMediaTypes"))
                .body(createApiBody())
                .bodyS3Location(S3LocationProperty.builder()
                        .bucket("bucket")
                        .eTag("eTag")
                        .key("key")
                        .version("version")
                        .build())
                //.cloneFrom("cloneFrom")
                .description("Sleeper Rest Api")
                //.disableExecuteApiEndpoint(false)
                //.endpointAccessMode("endpointAccessMode")
                .endpointConfiguration(EndpointConfigurationProperty.builder()
                        .ipAddressType("ipAddressType")
                        .types(List.of("types"))
                        .vpcEndpointIds(List.of("vpcEndpointIds"))
                        .build())
                .failOnWarnings(false)
                .minimumCompressionSize(123)
                //.mode("mode")
                //.name("name")
                //.parameters(Map.of("parametersKey", "parameters"))
                //.policy(policy)
                .securityPolicy("securityPolicy")
                .tags(List.of(CfnTag.builder()
                        .key("key")
                        .value("value")
                        .build()))
                .build();
    }

    //Method to add all functionality to rest api
    private Object createApiBody() {
        return createRestApiRequest("test-id", "getVersion");
    }

    private CreateRestApiRequest createRestApiRequest(String id, String name) {
        return CreateRestApiRequest.builder()
                .cloneFrom(id)
                .description("Get version of sleeper")
                .name(name)
                .build();
    }
}
