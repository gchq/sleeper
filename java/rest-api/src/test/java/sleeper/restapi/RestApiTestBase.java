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

import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent.RequestContext;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent.RequestContext.Http;
import org.junit.jupiter.api.BeforeEach;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.AddTable;

import static org.mockito.Mockito.mock;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

/**
 * Base class for universal actions across REST Api testing.
 */
public class RestApiTestBase {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    public final AddTable addTable = mock(AddTable.class);
    public RestApiLambda lambda;

    @BeforeEach
    void setUp() {
        lambda = new RestApiLambda(instanceProperties, addTable);
    }

    /**
     * Generates event behaviour for testing across REST Api actions.
     *
     * @param  method http method type
     * @param  path   rest endpoint to use
     * @param  body   body text to be sent
     * @return        event to be action by rest api
     */
    public APIGatewayV2HTTPEvent event(String method, String path, String body) {
        Http http = new Http();
        http.setMethod(method);
        http.setPath(path);
        RequestContext requestContext = new RequestContext();
        requestContext.setHttp(http);
        APIGatewayV2HTTPEvent event = new APIGatewayV2HTTPEvent();
        event.setRequestContext(requestContext);
        event.setBody(body);
        return event;
    }
}
