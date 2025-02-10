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
package sleeper.bulkexport.lambda;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import sleeper.bulkexport.core.model.BulkExportQuery;
import sleeper.bulkexport.core.model.BulkExportQuerySerDe;
import sleeper.bulkexport.core.model.BulkExportQueryValidationException;
import sleeper.configuration.properties.S3InstanceProperties;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.ObjectFactoryException;

/**
 * A lambda that is triggered when a serialised export query arrives on an SQS
 * queue. A processor executes the request and publishes the results to S3 based on.
 */
@SuppressWarnings("unused")
public class SqsBulkExportProcessorLambda implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsBulkExportProcessorLambda.class);

    private InstanceProperties instanceProperties;
    private final AmazonSQS sqsClient;
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoClient;
    private SqsBulkExportProcessor processor;
    private BulkExportQuerySerDe bulkExportQuerySerDe;

    /**
     * Constructs an instance of SqsBulkExportProcessorLambda using default clients for Amazon SQS, Amazon S3, and Amazon DynamoDB.
     *
     * @throws ObjectFactoryException if there is an error creating the object.
     */
    public SqsBulkExportProcessorLambda() throws ObjectFactoryException {
        this(AmazonSQSClientBuilder.defaultClient(), AmazonS3ClientBuilder.defaultClient(), AmazonDynamoDBClientBuilder.defaultClient());
    }

    /**
     * Constructs an instance of SqsBulkExportProcessorLambda.
     *
     * @param sqsClient The Amazon SQS client used for interacting with SQS.
     * @param s3Client The Amazon S3 client used for interacting with S3.
     * @param dynamoClient The Amazon DynamoDB client used for interacting with DynamoDB.
     * @throws ObjectFactoryException If there is an error creating the object.
     */
    public SqsBulkExportProcessorLambda(AmazonSQS sqsClient, AmazonS3 s3Client, AmazonDynamoDB dynamoClient) throws ObjectFactoryException {
        this.sqsClient = sqsClient;
        this.s3Client = s3Client;
        this.dynamoClient = dynamoClient;
        String bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, bucket);
        bulkExportQuerySerDe = new BulkExportQuerySerDe();
        processor = SqsBulkExportProcessor.builder()
        .sqsClient(sqsClient)
        .s3Client(s3Client)
        .dynamoClient(dynamoClient)
                .instanceProperties(instanceProperties)
                .build();
    }

    /**
     * Handles the incoming SQS event, processes each message, and performs the bulk export operation.
     *
     * @param event   The SQS event containing the messages to be processed.
     * @param context The AWS Lambda context object providing runtime information.
     * @return Always returns null.
     */
    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        event.getRecords().stream()
                .map(SQSEvent.SQSMessage::getBody)
                .peek(body -> LOGGER.info("Received message with body {}", body))
                .flatMap(body -> deserialiseAndValidate(body).stream())
                .peek(json -> LOGGER.debug(json.toString()))
                .forEach(query -> {
                    try {
                        processor.processExport(query);
                    } catch (ObjectFactoryException e) {
                        LOGGER.error("Failed to process export query", e);
                    }
                });
        return null;
    }

    /**
     * Deserialises a JSON string into a BulkExportQuery object and validates it.
     * If the deserialization or validation fails, logs the error and returns an empty Optional.
     *
     * @param message the JSON string representing the BulkExportQuery
     * @return an Optional containing the deserialized BulkExportQuery if successful, otherwise an empty Optional
     */
    public Optional<BulkExportQuery> deserialiseAndValidate(String message) {
        try {
            BulkExportQuery exportQuery = bulkExportQuerySerDe.fromJson(message);
            LOGGER.info("Deserialised message to query {}", exportQuery);
            return Optional.of(exportQuery);
        } catch (BulkExportQueryValidationException e) {
            LOGGER.error("QueryValidationException validating query from JSON {}", message, e);
            return Optional.empty();
        } catch (RuntimeException e) {
            LOGGER.error("Failed deserialising query from JSON {}", message, e);
            return Optional.empty();
        }
    }
}