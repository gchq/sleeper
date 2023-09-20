/*
 * Copyright 2022-2023 Crown Copyright
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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.google.gson.JsonParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.query.model.Query;
import sleeper.query.model.QuerySerDe;

import static sleeper.configuration.properties.instance.CommonProperty.FORCE_RELOAD_PROPERTIES;
import static sleeper.configuration.properties.instance.QueryProperty.QUERY_PROCESSING_LAMBDA_STATE_REFRESHING_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * A lambda that is triggered when a serialised query arrives on an SQS queue. A processor executes the request using a
 * {@link sleeper.query.executor.QueryExecutor} and publishes the results to either SQS or S3 based on the configuration of the query.
 * The processor contains a cache that includes mappings from partitions to files in those partitions. This is reused by
 * subsequent calls to the lambda if the AWS runtime chooses to reuse the instance.
 */
@SuppressWarnings("unused")
public class SqsQueryProcessorLambda implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsQueryProcessorLambda.class);

    private long lastUpdateTime;
    private InstanceProperties instanceProperties;
    private final AmazonSQS sqsClient;
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoClient;
    private QuerySerDe serde;
    private SqsQueryProcessor processor;

    public SqsQueryProcessorLambda() throws ObjectFactoryException {
        this(AmazonS3ClientBuilder.defaultClient(), AmazonSQSClientBuilder.defaultClient(),
                AmazonDynamoDBClientBuilder.defaultClient(), System.getenv(CONFIG_BUCKET.toEnvironmentVariable()));
    }

    public SqsQueryProcessorLambda(AmazonS3 s3Client, AmazonSQS sqsClient, AmazonDynamoDB dynamoClient, String configBucket) throws ObjectFactoryException {
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

        for (SQSEvent.SQSMessage message : event.getRecords()) {
            LOGGER.info("Received message with body {}", message.getBody());
            Query query;
            try {
                query = serde.fromJson(message.getBody());
                LOGGER.info("Deserialised message to query {}", query);
            } catch (JsonParseException e) {
                LOGGER.error("JSONParseException deserialsing query from JSON {}", message.getBody());
                continue;
            }
            processor.processQuery(query);
        }
        return null;
    }

    private void updateStateIfNecessary() throws ObjectFactoryException {
        double timeSinceLastUpdatedInSeconds = (System.currentTimeMillis() - lastUpdateTime) / 1000.0;
        int stateRefreshingPeriod = instanceProperties.getInt(QUERY_PROCESSING_LAMBDA_STATE_REFRESHING_PERIOD_IN_SECONDS);
        if (timeSinceLastUpdatedInSeconds > stateRefreshingPeriod || instanceProperties.getBoolean(FORCE_RELOAD_PROPERTIES)) {
            LOGGER.info("Mapping of partition to files was last updated {} seconds ago, so refreshing", timeSinceLastUpdatedInSeconds);
            updateProperties(instanceProperties.get(CONFIG_BUCKET));
        }
    }

    private void updateProperties(String configBucket) throws ObjectFactoryException {
        // Refresh properties and caches
        if (null == configBucket) {
            LOGGER.error("Config Bucket was null. Was an environment variable missing?");
            throw new RuntimeException("Error: can't find S3 bucket from environment variable");
        }
        instanceProperties = loadInstanceProperties(s3Client, configBucket);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        serde = new QuerySerDe(tablePropertiesProvider);
        processor = SqsQueryProcessor.builder()
                .sqsClient(sqsClient).s3Client(s3Client).dynamoClient(dynamoClient)
                .instanceProperties(instanceProperties).tablePropertiesProvider(tablePropertiesProvider)
                .build();
        lastUpdateTime = System.currentTimeMillis();
    }

    private static InstanceProperties loadInstanceProperties(AmazonS3 s3Client, String configBucket) {
        InstanceProperties properties = new InstanceProperties();
        properties.loadFromS3(s3Client, configBucket);
        return properties;
    }

}
