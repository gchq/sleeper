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
package sleeper.restapi;

import software.amazon.awscdk.NestedStack;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.apigateway.ApiGatewayClient;
import software.amazon.awssdk.services.apigateway.model.ApiGatewayException;
import software.amazon.awssdk.services.apigateway.model.CreateRestApiRequest;
import software.amazon.awssdk.services.apigateway.model.CreateRestApiResponse;
import software.constructs.Construct;

import sleeper.cdk.artefacts.SleeperInstanceArtefacts;
import sleeper.cdk.lambda.SleeperLambdaCode;

/**
 * Rest api for interacting with the sleeper instance.
 * Utilises API Gateway.
 *
 * TODO Futher expand this javadoc
 */
public class SleeperRestAPI extends NestedStack {

    public SleeperRestAPI(Construct scope, String id, SleeperInstanceArtefacts artefacts) {
        super(scope, id);
        SleeperLambdaCode lambdaCode = artefacts.lambdaCodeAtScope(this);
        setUpRestApi(id, lambdaCode);
    }

    private void setUpRestApi(String id, SleeperLambdaCode lambdaCode) {
        //This section to elobrate/set from elsewhere
        String restApiName = "rest-api-name";
        Region region = Region.EU_WEST_2;

        ApiGatewayClient apiGateway = setUpApiGateway(region);

        try {
            CreateRestApiRequest request = CreateRestApiRequest.builder()
                    .cloneFrom(id)
                    .description("Rest api for sleeper interaction")
                    .name(restApiName)
                    .build();
            CreateRestApiResponse response = apiGateway.createRestApi(request);
            response.id();
        } catch (ApiGatewayException ex) {

        }

    }

    private ApiGatewayClient setUpApiGateway(Region region) {
        return ApiGatewayClient.builder()
                .region(region)
                .build();
    }

    /**
     * TODO add further expanded javadoc.
     */
    public void getVersion() {
        // Simple first method for providing functionality of returning the version number
    }
}
