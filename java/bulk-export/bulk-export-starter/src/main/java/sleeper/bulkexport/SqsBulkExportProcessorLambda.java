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

import java.time.Instant;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import sleeper.bulkexport.model.BulkExportQueryOrLeafPartitionQuery;
import sleeper.bulkexport.model.BulkExportQuerySerDe;
import sleeper.bulkexport.model.BulkExportQueryValidationException;
import sleeper.core.util.ObjectFactoryException;
import sleeper.core.properties.instance.InstanceProperties;

/**
 * A lambda that is triggered when a serialised export query arrives on an SQS
 * queue. A
 * processor executes the request and publishes the results to S3 based on.
 */
@SuppressWarnings("unused")
public class SqsBulkExportProcessorLambda implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsBulkExportProcessorLambda.class);

    private Instant lastUpdateTime;
    private InstanceProperties instanceProperties;
    private final AmazonSQS sqsClient;
    private SqsBulkExportProcessor processor;
    private BulkExportQuerySerDe bulkExportQuerySerDe;

    public SqsBulkExportProcessorLambda() throws ObjectFactoryException {
        this(AmazonSQSClientBuilder.defaultClient());
    }

    public SqsBulkExportProcessorLambda(AmazonSQS sqsClient) throws ObjectFactoryException {
        this.sqsClient = sqsClient;
        bulkExportQuerySerDe = new BulkExportQuerySerDe();
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

    public Optional<BulkExportQueryOrLeafPartitionQuery> deserialiseAndValidate(String message) {
        try {
            BulkExportQueryOrLeafPartitionQuery exportQuery = bulkExportQuerySerDe.fromJson(message);
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
