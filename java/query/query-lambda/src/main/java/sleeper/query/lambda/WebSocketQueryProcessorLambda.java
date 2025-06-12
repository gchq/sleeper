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
package sleeper.query.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.apigatewaymanagementapi.ApiGatewayManagementApiClient;
import software.amazon.awssdk.services.apigatewaymanagementapi.model.PostToConnectionRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QuerySerDe;
import sleeper.query.core.output.ResultsOutputConstants;
import sleeper.query.core.tracker.QueryStatusReportListener;
import sleeper.query.runner.output.WebSocketOutput;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unused")
public class WebSocketQueryProcessorLambda implements RequestHandler<APIGatewayV2WebSocketEvent, APIGatewayV2WebSocketResponse> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketQueryProcessorLambda.class);

    private final QuerySerDe serde;
    private final SqsClient sqsClient;
    private final String queryQueueUrl;

    public WebSocketQueryProcessorLambda() {
        this(
                S3Client.create(),
                DynamoDbClient.create(),
                SqsClient.create(),
                System.getenv(CdkDefinedInstanceProperty.CONFIG_BUCKET.toEnvironmentVariable()));
    }

    public WebSocketQueryProcessorLambda(S3Client s3Client, DynamoDbClient dynamoClient, SqsClient sqsClient, String configBucket) {
        this.sqsClient = sqsClient;
        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucket);
        this.queryQueueUrl = instanceProperties.get(CdkDefinedInstanceProperty.QUERY_QUEUE_URL);
        TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
        this.serde = new QuerySerDe(tablePropertiesProvider);
    }

    private void sendErrorToClient(String endpoint, String region, String connectionId, String errorMessage) {

        ApiGatewayManagementApiClient client = ApiGatewayManagementApiClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.of(region))
                .build();

        String data = "{\"message\":\"error\",\"error\":\"" + errorMessage + "\"}";
        PostToConnectionRequest request = PostToConnectionRequest.builder()
                .connectionId(connectionId)
                .data(SdkBytes.fromByteBuffer(ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)))).build();

        client.postToConnection(request);
    }

    public void submitQueryForProcessing(Query query) {
        String message = serde.toJson(query);

        sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(queryQueueUrl)
                .messageBody(message)
                .build());
    }

    @Override
    public APIGatewayV2WebSocketResponse handleRequest(APIGatewayV2WebSocketEvent event, Context context) {
        LOGGER.info("Received WebSocket event: {}", event);

        if (event.getRequestContext().getEventType().equals("MESSAGE")) {
            String region = System.getenv("AWS_REGION");
            if (region == null) {
                throw new RuntimeException("Unable to detect current region!");
            }
            String endpoint = "https://" + event.getRequestContext().getApiId() + ".execute-api." + region + ".amazonaws.com/" + event.getRequestContext().getStage();

            Query query = null;
            try {
                query = serde.fromJson(event.getBody());
                LOGGER.info("Deserialised message to query: {}", query);
            } catch (RuntimeException e) {
                LOGGER.error("Failed to deserialise query", e);
                this.sendErrorToClient(endpoint, region, event.getRequestContext().getConnectionId(), "Received malformed query JSON request");
            }

            if (query != null) {
                Map<String, String> statusReportDestination = new HashMap<>();
                statusReportDestination.put(QueryStatusReportListener.DESTINATION, WebSocketOutput.DESTINATION_NAME);
                statusReportDestination.put(WebSocketOutput.ENDPOINT, endpoint);
                statusReportDestination.put(WebSocketOutput.CONNECTION_ID, event.getRequestContext().getConnectionId());
                query = query.withStatusReportDestination(statusReportDestination);

                // Default to sending results back to client via WebSocket connection
                if (query.getResultsPublisherConfig().get(ResultsOutputConstants.DESTINATION) == null ||
                        query.getResultsPublisherConfig().get(ResultsOutputConstants.DESTINATION).equals(WebSocketOutput.DESTINATION_NAME)) {
                    query.getResultsPublisherConfig().put(ResultsOutputConstants.DESTINATION, WebSocketOutput.DESTINATION_NAME);
                    query.getResultsPublisherConfig().put(WebSocketOutput.ENDPOINT, endpoint);
                    query.getResultsPublisherConfig().put(WebSocketOutput.CONNECTION_ID, event.getRequestContext().getConnectionId());
                }

                LOGGER.info("Query to be processed: {}", query);
                submitQueryForProcessing(query);
            }
        }

        APIGatewayV2WebSocketResponse response = new APIGatewayV2WebSocketResponse();
        response.setStatusCode(200);
        return response;
    }
}
