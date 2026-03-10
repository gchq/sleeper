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
import software.amazon.awssdk.services.apigateway.ApiGatewayClient;
import software.amazon.awssdk.services.apigateway.model.CreateRestApiRequest;
import software.amazon.awssdk.services.apigateway.model.CreateRestApiResponse;
import software.constructs.Construct;

/**
 * Rest api for interacting with the sleeper instance.
 * Utilises API Gateway.
 *
 * TODO Futher expand this javadoc
 */
public class SleeperRestAPI extends NestedStack {

    public SleeperRestAPI(Construct scope, String id) {
        super(scope, id);
    }

    private ApiGatewayClient setUpApiGateway() {
        return null;
    }

    private void createRestApi(ApiGatewayClient apiGateway, String restApiId) {
        try {
            CreateRestApiRequest request = CreateRestApiRequest.builder()
                    .cloneFrom(restApiId)
                    .description("test rest api") //TODO Elobate description
                    .name("SleeperRestAPI") // TODO allow customisation
                    .build();
            CreateRestApiResponse response = apiGateway.createRestApi(request);
            response.id();
        } catch (Exception ex) {

        }
    }

    /**
     * TODO add further expanded javadoc.
     */
    public void getVersion() {
        // Simple first method for providing functionality of returning the version number
    }
}
