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
package sleeper.garbagecollector;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.table.InvokeForTableRequestSerDe;
import sleeper.core.util.LoggedDuration;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreProvider;

import java.time.Instant;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * Runs the garbage collector in AWS Lambda. Builds and invokes {@link GarbageCollector} for a batch of tables.
 */
@SuppressWarnings("unused")
public class GarbageCollectorLambda implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GarbageCollectorLambda.class);

    private final InvokeForTableRequestSerDe serDe = new InvokeForTableRequestSerDe();
    private final TablePropertiesProvider tablePropertiesProvider;
    private final GarbageCollector garbageCollector;

    public GarbageCollectorLambda() {
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClient.builder().build();

        String s3Bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        if (null == s3Bucket) {
            throw new RuntimeException("Couldn't get S3 bucket from environment variable");
        }

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, s3Bucket);
        LOGGER.debug("Loaded InstanceProperties from {}", s3Bucket);

        tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient);
        Configuration conf = HadoopConfigurationProvider.getConfigurationForLambdas(instanceProperties);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, conf);

        this.garbageCollector = new GarbageCollector(conf,
                instanceProperties, tablePropertiesProvider, stateStoreProvider);
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        Instant startTime = Instant.now();
        LOGGER.info("Lambda started at {}", startTime);

        try {
            event.getRecords().stream()
                    .map(SQSEvent.SQSMessage::getBody)
                    .peek(body -> LOGGER.info("Received message: {}", body))
                    .map(serDe::fromJson)
                    .forEach(garbageCollector::run);
        } finally {
            Instant finishTime = Instant.now();
            LOGGER.info("Lambda finished at {} (ran for {})",
                    finishTime, LoggedDuration.withFullOutput(startTime, finishTime));
        }
        return null;
    }
}
