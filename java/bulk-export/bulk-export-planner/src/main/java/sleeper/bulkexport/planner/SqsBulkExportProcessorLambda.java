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

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.bulkexport.core.model.BulkExportQuery;
import sleeper.bulkexport.core.model.BulkExportQuerySerDe;
import sleeper.bulkexport.core.model.BulkExportQueryValidationException;
import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.util.ObjectFactoryException;
import sleeper.statestore.StateStoreFactory;

import java.util.Optional;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * A lambda that is triggered when a serialised export query arrives on an SQS
 * queue. The lambda processes the query and splits the export into leaf
 * partition queries.
 */
@SuppressWarnings("unused")
public class SqsBulkExportProcessorLambda implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsBulkExportProcessorLambda.class);
    private final InstanceProperties instanceProperties;
    private final SqsBulkExportProcessor processor;
    private final BulkExportQuerySerDe bulkExportQuerySerDe;

    public SqsBulkExportProcessorLambda() throws ObjectFactoryException {
        this(SqsClient.create(),
                S3Client.create(),
                DynamoDbClient.create(), System.getenv(CONFIG_BUCKET.toEnvironmentVariable()));
    }

    public SqsBulkExportProcessorLambda(
            SqsBulkExportProcessor processor,
            BulkExportQuerySerDe bulkExportQuerySerDe,
            InstanceProperties instanceProperties) {
        this.processor = processor;
        this.bulkExportQuerySerDe = bulkExportQuerySerDe;
        this.instanceProperties = instanceProperties;
    }

    public SqsBulkExportProcessorLambda(SqsClient sqsClient, S3Client s3Client, DynamoDbClient dynamoClient, String configBucket)
            throws ObjectFactoryException {
        instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucket);
        this.bulkExportQuerySerDe = new BulkExportQuerySerDe();
        TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(
                S3InstanceProperties.loadFromBucket(s3Client, configBucket), s3Client, dynamoClient);
        processor = SqsBulkExportProcessor.builder()
                .sqsClient(sqsClient)
                .instanceProperties(instanceProperties)
                .tablePropertiesProvider(tablePropertiesProvider)
                .stateStoreProvider(StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient))
                .build();
    }

    public SqsBulkExportProcessorLambda(SqsClient sqsClient, S3Client s3Client, DynamoDbClient dynamoClient, String configBucket, TablePropertiesProvider tablePropertiesProvider)
            throws ObjectFactoryException {
        bulkExportQuerySerDe = new BulkExportQuerySerDe();
        instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucket);
        processor = SqsBulkExportProcessor.builder()
                .sqsClient(sqsClient)
                .instanceProperties(instanceProperties)
                .tablePropertiesProvider(tablePropertiesProvider)
                .stateStoreProvider(StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient))
                .build();
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        event.getRecords().stream()
                .map(SQSEvent.SQSMessage::getBody)
                .peek(body -> LOGGER.debug("Received message with body {}", body))
                .flatMap(body -> {
                    return deserialiseAndValidate(body).stream();
                })
                .forEach(query -> {
                    try {
                        processor.processExport(query);
                    } catch (ObjectFactoryException e) {
                        LOGGER.error("Failed to process export query", e);
                        throw new RuntimeException(e);
                    }
                });
        return null;
    }

    private Optional<BulkExportQuery> deserialiseAndValidate(String message) {
        try {
            BulkExportQuery exportQuery = bulkExportQuerySerDe.fromJson(message);
            LOGGER.debug("Deserialised message to export query {}", exportQuery);
            return Optional.of(exportQuery);
        } catch (BulkExportQueryValidationException e) {
            LOGGER.error("QueryValidationException validating export query from JSON {}", message, e);
            throw e;
        } catch (RuntimeException e) {
            LOGGER.error("Failed deserialising export query from JSON {}", message, e);
            throw e;
        }
    }
}
