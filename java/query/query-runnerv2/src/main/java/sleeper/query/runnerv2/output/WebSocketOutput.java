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
package sleeper.query.runnerv2.output;

public class WebSocketOutput {
    public static final String DESTINATION_NAME = "WEBSOCKET";
    public static final String REGION = "webSocketManagementApiRegion";
    public static final String ENDPOINT = "webSocketManagementApiEndpoint";
    public static final String CONNECTION_ID = "webSocketConnectionId";
    public static final String ACCESS_KEY = "awsAccessKey";
    public static final String SECRET_KEY = "awsSecretKey";
    public static final String MAX_BATCH_SIZE = "maxBatchSize";

    private WebSocketOutput() {
    }
}
