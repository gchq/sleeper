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

import sleeper.bulkexport.model.BulkExportQuery;
import sleeper.bulkexport.model.BulkExportQuerySerDe;
import sleeper.bulkexport.model.BulkExportQueryValidationException;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.core.properties.instance.InstanceProperties;

/**
 * TODO Update
 * A lambda that is triggered when a serialised query arrives on an SQS queue. A processor executes the request using a
 * {@link QueryExecutor} and publishes the results to either SQS or S3 based on the configuration of the query.
 * The processor contains a cache that includes mappings from partitions to files in those partitions. This is reused by
 * subsequent calls to the lambda if the AWS runtime chooses to reuse the instance.
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
                .peek(json -> LOGGER.debug(json.toString()));
        //.forEach(processor::processQuery);
        return null;
    }

    public Optional<BulkExportQuery> deserialiseAndValidate(String message) {
        try {
            BulkExportQuery query = bulkExportQuerySerDe.fromJson(message);
            LOGGER.info("Deserialised message to query {}", query);
            return Optional.of(query);
        } catch (BulkExportQueryValidationException e) {
            LOGGER.error("QueryValidationException validating query from JSON {}", message, e);
            return Optional.empty();
        } catch (RuntimeException e) {
            LOGGER.error("Failed deserialising query from JSON {}", message, e);
            return Optional.empty();
        }
    }
}
