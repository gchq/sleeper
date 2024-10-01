/*
 * Copyright 2022-2024 Crown Copyright
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

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.apigatewaymanagementapi.AmazonApiGatewayManagementApi;
import com.amazonaws.services.apigatewaymanagementapi.AmazonApiGatewayManagementApiClientBuilder;
import com.amazonaws.services.apigatewaymanagementapi.model.PostToConnectionRequest;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketResponse;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.query.model.Query;
import sleeper.query.model.QuerySerDe;
import sleeper.query.output.ResultsOutputConstants;
import sleeper.query.runner.output.WebSocketResultsOutput;
import sleeper.query.runner.tracker.WebSocketQueryStatusReportDestination;
import sleeper.query.tracker.QueryStatusReportListener;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unused")
public class WebSocketQueryProcessorLambda implements RequestHandler<APIGatewayV2WebSocketEvent, APIGatewayV2WebSocketResponse> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketQueryProcessorLambda.class);

    private final QuerySerDe serde;
    private final AmazonSQS sqsClient;
    private final String queryQueueUrl;

    public WebSocketQueryProcessorLambda() {
        this(
                AmazonS3ClientBuilder.defaultClient(),
                AmazonDynamoDBClientBuilder.defaultClient(),
                AmazonSQSClientBuilder.defaultClient(),
                System.getenv(CdkDefinedInstanceProperty.CONFIG_BUCKET.toEnvironmentVariable()));
    }

    public WebSocketQueryProcessorLambda(AmazonS3 s3Client, AmazonDynamoDB dynamoClient, AmazonSQS sqsClient, String configBucket) {
        this.sqsClient = sqsClient;
        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucket);
        this.queryQueueUrl = instanceProperties.get(CdkDefinedInstanceProperty.QUERY_QUEUE_URL);
        TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
        this.serde = new QuerySerDe(tablePropertiesProvider);
    }

    private void sendErrorToClient(String endpoint, String region, String connectionId, String errorMessage) {
        AmazonApiGatewayManagementApi client = AmazonApiGatewayManagementApiClientBuilder.standard()
                .withEndpointConfiguration(new EndpointConfiguration(endpoint, region))
                .build();

        String data = "{\"message\":\"error\",\"error\":\"" + errorMessage + "\"}";
        PostToConnectionRequest request = new PostToConnectionRequest()
                .withConnectionId(connectionId)
                .withData(ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)));

        client.postToConnection(request);
    }

    public void submitQueryForProcessing(Query query) {
        String message = serde.toJson(query);
        sqsClient.sendMessage(queryQueueUrl, message);
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
                statusReportDestination.put(QueryStatusReportListener.DESTINATION, WebSocketQueryStatusReportDestination.DESTINATION_NAME);
                statusReportDestination.put(WebSocketQueryStatusReportDestination.ENDPOINT, endpoint);
                statusReportDestination.put(WebSocketQueryStatusReportDestination.CONNECTION_ID, event.getRequestContext().getConnectionId());
                query = query.withStatusReportDestination(statusReportDestination);

                // Default to sending results back to client via WebSocket connection
                if (query.getResultsPublisherConfig().get(ResultsOutputConstants.DESTINATION) == null ||
                        query.getResultsPublisherConfig().get(ResultsOutputConstants.DESTINATION).equals(WebSocketResultsOutput.DESTINATION_NAME)) {
                    query.getResultsPublisherConfig().put(ResultsOutputConstants.DESTINATION, WebSocketResultsOutput.DESTINATION_NAME);
                    query.getResultsPublisherConfig().put(WebSocketResultsOutput.ENDPOINT, endpoint);
                    query.getResultsPublisherConfig().put(WebSocketResultsOutput.CONNECTION_ID, event.getRequestContext().getConnectionId());
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
