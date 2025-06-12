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
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.configuration.jars.S3UserJarsLoader;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.util.LoggedDuration;
import sleeper.core.util.ObjectFactoryException;
import sleeper.query.core.recordretrieval.QueryExecutor;
import sleeper.query.runner.tracker.DynamoDBQueryTracker;
import sleeper.statestore.StateStoreFactory;

import java.nio.file.Paths;
import java.time.Instant;
import java.util.concurrent.Executors;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FORCE_RELOAD_PROPERTIES;
import static sleeper.core.properties.instance.QueryProperty.QUERY_PROCESSING_LAMBDA_STATE_REFRESHING_PERIOD_IN_SECONDS;
import static sleeper.core.properties.instance.QueryProperty.QUERY_PROCESSOR_LAMBDA_RECORD_RETRIEVAL_THREADS;

/**
 * A lambda that is triggered when a serialised query arrives on an SQS queue. A processor executes the request using a
 * {@link QueryExecutor} and publishes the results to either SQS or S3 based on the configuration of the query.
 * The processor contains a cache that includes mappings from partitions to files in those partitions. This is reused by
 * subsequent calls to the lambda if the AWS runtime chooses to reuse the instance.
 */
@SuppressWarnings("unused")
public class SqsQueryProcessorLambda implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsQueryProcessorLambda.class);

    private Instant lastUpdateTime;
    private InstanceProperties instanceProperties;
    private final SqsClient sqsClient;
    private final S3Client s3Client;
    private final DynamoDbClient dynamoClient;
    private QueryMessageHandler messageHandler;
    private SqsQueryProcessor processor;

    public SqsQueryProcessorLambda() throws ObjectFactoryException {
        this(S3Client.create(), SqsClient.create(),
                DynamoDbClient.create(),
                System.getenv(CONFIG_BUCKET.toEnvironmentVariable()));
    }

    public SqsQueryProcessorLambda(S3Client s3Client, SqsClient sqsClient, DynamoDbClient dynamoClient,
            String configBucket) throws ObjectFactoryException {
        this.s3Client = s3Client;
        this.sqsClient = sqsClient;
        this.dynamoClient = dynamoClient;
        updateProperties(configBucket);
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        try {
            updateStateIfNecessary();
        } catch (ObjectFactoryException e) {
            throw new RuntimeException("ObjectFactoryException updating state", e);
        }

        event.getRecords().stream()
                .map(SQSEvent.SQSMessage::getBody)
                .peek(body -> LOGGER.info("Received message with body {}", body))
                .flatMap(body -> messageHandler.deserialiseAndValidate(body).stream())
                .forEach(processor::processQuery);
        return null;
    }

    private void updateStateIfNecessary() throws ObjectFactoryException {
        LoggedDuration duration = LoggedDuration.withFullOutput(lastUpdateTime, Instant.now());
        long timeSinceLastUpdatedInSeconds = duration.getSeconds();
        int stateRefreshingPeriod = instanceProperties.getInt(QUERY_PROCESSING_LAMBDA_STATE_REFRESHING_PERIOD_IN_SECONDS);
        if (timeSinceLastUpdatedInSeconds > stateRefreshingPeriod || instanceProperties.getBoolean(FORCE_RELOAD_PROPERTIES)) {
            LOGGER.info("Mapping of partition to files was last updated {} ago, so refreshing", duration);
            updateProperties(instanceProperties.get(CONFIG_BUCKET));
        }
    }

    private void updateProperties(String configBucket) throws ObjectFactoryException {
        // Refresh properties and caches
        if (null == configBucket) {
            LOGGER.error("Config Bucket was null. Was an environment variable missing?");
            throw new RuntimeException("Error: can't find S3 bucket from environment variable");
        }
        instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucket);
        TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
        messageHandler = new QueryMessageHandler(tablePropertiesProvider, new DynamoDBQueryTracker(instanceProperties, dynamoClient));
        processor = SqsQueryProcessor.builder()
                .sqsClient(sqsClient)
                .instanceProperties(instanceProperties).tablePropertiesProvider(tablePropertiesProvider)
                .stateStoreProvider(StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient))
                .executorService(Executors.newFixedThreadPool(instanceProperties.getInt(QUERY_PROCESSOR_LAMBDA_RECORD_RETRIEVAL_THREADS)))
                .objectFactory(new S3UserJarsLoader(instanceProperties, s3Client, Paths.get("/tmp")).buildObjectFactory())
                .queryListener(new DynamoDBQueryTracker(instanceProperties, dynamoClient))
                .build();
        lastUpdateTime = Instant.now();
    }
}
