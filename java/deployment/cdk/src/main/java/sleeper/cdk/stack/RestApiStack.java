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

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.apigateway.ApiGatewayClient;
import software.amazon.awssdk.services.apigateway.model.ApiGatewayException;
import software.amazon.awssdk.services.apigateway.model.CreateRestApiRequest;
import software.amazon.awssdk.services.apigateway.model.CreateRestApiResponse;
import software.constructs.Construct;

import sleeper.cdk.artefacts.SleeperInstanceArtefacts;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
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
        setUpRestApi(instanceProperties, instanceProperties.get(ID), lambdaCode);
    }

    private void setUpRestApi(InstanceProperties instanceProperties, String id, SleeperLambdaCode lambdaCode) {
        //This section to elobrate/set from elsewhere
        String restApiName = "rest-api-name";

        Map<String, String> env = EnvironmentUtils.createDefaultEnvironment(instanceProperties);
        String functionName = String.join("-", "sleeper", id, "rest-api-handler");
        IFunction lambdaFunction = lambdaCode.buildFunction(LambdaHandler.REST_API_HANDLER, id, builder -> builder
                .functionName(functionName)
                .description("Function for creating rest api for interacting with sleeper")
                .environment(env)
                .memorySize(1024)
                // Need a log group
                .timeout(Duration.seconds(60)));

        ApiGatewayClient apiGateway = setUpApiGateway(Region.of(instanceProperties.get(REGION)));

        CreateRestApiRequest request = createRestApiRequest(id, restApiName);
        try {
            CreateRestApiResponse response = apiGateway.createRestApi(request);
            response.id(); // Just doing something with the response to stop the redline for now
        } catch (ApiGatewayException ex) {

        }

    }

    private ApiGatewayClient setUpApiGateway(Region region) {
        return ApiGatewayClient.builder()
                .region(region)
                .build();
    }

    private CreateRestApiRequest createRestApiRequest(String id, String name) {
        return CreateRestApiRequest.builder()
                .cloneFrom(id)
                .description("Rest api for sleeper interaction")
                .name(name)
                .build();
    }

    /**
     * TODO add further expanded javadoc.
     */
    public void getVersion() {
        // Simple first method for providing functionality of returning the version number
    }
}
