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
package sleeper.bulkexport.planner;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkexport.core.model.BulkExportQuery;
import sleeper.bulkexport.core.model.BulkExportQuerySerDe;
import sleeper.bulkexport.core.model.BulkExportQueryValidationException;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.util.ObjectFactoryException;

import java.util.Optional;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_QUEUE_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * A lambda that is triggered when a serialised export query arrives on an SQS
 * queue. The lambda processes the query and splits the export into leaf
 * partition queries.
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

    public SqsBulkExportProcessorLambda() throws ObjectFactoryException {
        this(AmazonSQSClientBuilder.defaultClient(), AmazonS3ClientBuilder.defaultClient(),
                AmazonDynamoDBClientBuilder.defaultClient(), System.getenv(CONFIG_BUCKET.toEnvironmentVariable()));
    }

    public SqsBulkExportProcessorLambda(
            SqsBulkExportProcessor processor,
            BulkExportQuerySerDe bulkExportQuerySerDe,
            InstanceProperties instanceProperties,
            AmazonSQS sqsClient,
            AmazonS3 s3Client,
            AmazonDynamoDB dynamoClient) {
        this.processor = processor;
        this.bulkExportQuerySerDe = bulkExportQuerySerDe;
        this.instanceProperties = instanceProperties;
        this.sqsClient = sqsClient;
        this.s3Client = s3Client;
        this.dynamoClient = dynamoClient;
    }

    public SqsBulkExportProcessorLambda(AmazonSQS sqsClient, AmazonS3 s3Client, AmazonDynamoDB dynamoClient, String configBucket)
            throws ObjectFactoryException {
        this(sqsClient, s3Client, dynamoClient, configBucket, S3TableProperties.createProvider(
                S3InstanceProperties.loadFromBucket(s3Client, configBucket), s3Client, dynamoClient));
        instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucket);
    }

    public SqsBulkExportProcessorLambda(AmazonSQS sqsClient, AmazonS3 s3Client, AmazonDynamoDB dynamoClient, String configBucket, TablePropertiesProvider tablePropertiesProvider)
            throws ObjectFactoryException {
        this.sqsClient = sqsClient;
        this.s3Client = s3Client;
        this.dynamoClient = dynamoClient;
        this.bulkExportQuerySerDe = new BulkExportQuerySerDe();

        instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucket);
        processor = SqsBulkExportProcessor.builder()
                .sqsClient(sqsClient)
                .s3Client(s3Client)
                .dynamoClient(dynamoClient)
                .instanceProperties(instanceProperties)
                .tablePropertiesProvider(tablePropertiesProvider)
                .build();
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        event.getRecords().stream()
                .map(SQSEvent.SQSMessage::getBody)
                .peek(body -> LOGGER.debug("Received message with body {}", body))
                .flatMap(body -> {
                    try {
                        return deserialiseAndValidate(body).stream();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .forEach(query -> {
                    try {
                        processor.processExport(query);
                    } catch (ObjectFactoryException e) {
                        LOGGER.error("Failed to process export query", e);
                        sendToDeadLetterQueue(query.toString());
                        throw new RuntimeException(e);
                    }
                });
        return null;
    }

    private Optional<BulkExportQuery> deserialiseAndValidate(String message) throws Exception {
        try {
            BulkExportQuery exportQuery = bulkExportQuerySerDe.fromJson(message);
            LOGGER.debug("Deserialised message to export query {}", exportQuery);
            return Optional.of(exportQuery);
        } catch (BulkExportQueryValidationException e) {
            LOGGER.error("QueryValidationException validating export query from JSON {}", message, e);
            sendToDeadLetterQueue(message);
            throw e;
        } catch (RuntimeException e) {
            LOGGER.error("Failed deserialising export query from JSON {}", message, e);
            sendToDeadLetterQueue(message);
            throw e;
        }
    }

    /**
     * Sends the given message to the Dead Letter Queue (DLQ).
     *
     * @param message The message to send to the DLQ.
     */
    public void sendToDeadLetterQueue(String message) {
        String dlqUrl = instanceProperties.get(BULK_EXPORT_QUEUE_DLQ_URL);
        if (dlqUrl == null || dlqUrl.isEmpty()) {
            LOGGER.warn("No DLQ URL found in instance properties");
            return;
        }
        LOGGER.warn("Sending message to DLQ: {}", message);
        sqsClient.sendMessage(dlqUrl, message);
    }
}
