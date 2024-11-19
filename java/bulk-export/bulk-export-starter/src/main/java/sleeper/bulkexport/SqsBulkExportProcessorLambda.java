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
package sleeper.bulkexport;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkexport.model.BulkExportQuery;
import sleeper.bulkexport.model.BulkExportQuerySerDe;
import sleeper.bulkexport.model.BulkExportQueryValidationException;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.ObjectFactoryException;

import java.util.Optional;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * A lambda that is triggered when a serialised export query arrives on an SQS
 * queue. A
 * processor executes the request and publishes the results to S3 based on.
 */
@SuppressWarnings("unused")
public class SqsBulkExportProcessorLambda implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsBulkExportProcessorLambda.class);

    private InstanceProperties instanceProperties;
    private final AmazonSQS sqsClient;
    private final AmazonS3 s3Client;
    private SqsBulkExportProcessor processor;
    private BulkExportQuerySerDe bulkExportQuerySerDe;

    public SqsBulkExportProcessorLambda() throws ObjectFactoryException {
        this(AmazonSQSClientBuilder.defaultClient(), AmazonS3ClientBuilder.defaultClient());
    }

    public SqsBulkExportProcessorLambda(AmazonSQS sqsClient, AmazonS3 s3Client) throws ObjectFactoryException {
        this.sqsClient = sqsClient;
        this.s3Client = s3Client;
        String bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, bucket);
        bulkExportQuerySerDe = new BulkExportQuerySerDe();
        processor = SqsBulkExportProcessor.builder()
                .sqsClient(sqsClient)
                .instanceProperties(instanceProperties)
                .build();
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        event.getRecords().stream()
                .map(SQSEvent.SQSMessage::getBody)
                .peek(body -> LOGGER.info("Received message with body {}", body))
                .flatMap(body -> deserialiseAndValidate(body).stream())
                .peek(json -> LOGGER.debug(json.toString()))
                .forEach(processor::processExport);
        return null;
    }

    /**
     * Deserialise and validate the bulk export query.
     *
     * @param message the bulk export message.
     *
     * @return deserialised BulkExportQuery.
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
