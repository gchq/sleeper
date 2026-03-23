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
package sleeper.restapi;

import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;

/**
 * TODO Add class javadoc.
 */
public class RestApiLambda {

    public RestApiLambda() {
    }

    /**
     * TODO Add javadoc later.
     *
     * @param event words
     */
    public void handleEvent(APIGatewayV2HTTPEvent event) {
        // Properties aren't used yet but are likely required for future features
        // Map<String, Object> resourceProperties = event.getResourceProperties();

        /*
         * switch (event.getRequestType()) {
         * case "getVersion":
         * getVersion();
         * break;
         * default:
         * throw new IllegalArgumentException("Invalid request type: " + event.getRequestType());
         */

    }

    private void getVersion() {
        // TODO: Placeholder method for action of getVersion from the RESTapi
    }
}
