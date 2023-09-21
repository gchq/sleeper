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
package sleeper.garbagecollector;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import sleeper.table.job.TableLister;
import sleeper.utils.HadoopConfigurationProvider;

import java.io.IOException;
import java.time.LocalDateTime;

import static sleeper.configuration.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_BATCH_SIZE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * A lambda for executing the {@link GarbageCollector}.
 */
@SuppressWarnings("unused")
public class GarbageCollectorLambda {
    private static final Logger LOGGER = LoggerFactory.getLogger(GarbageCollectorLambda.class);

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

        TableLister tableLister = new TableLister(s3Client, instanceProperties);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        Configuration conf = HadoopConfigurationProvider.getConfigurationForLambdas(instanceProperties);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, conf);

        this.garbageCollector = new GarbageCollector(conf,
                tableLister,
                tablePropertiesProvider,
                stateStoreProvider,
                instanceProperties.getInt(GARBAGE_COLLECTOR_BATCH_SIZE));
    }

    public void eventHandler(ScheduledEvent event, Context context) {
        LOGGER.info("GarbageCollectorLambda lambda triggered at {}", event.getTime());

        try {
            garbageCollector.run();
        } catch (IOException | StateStoreException e) {
            LOGGER.error("Exception thrown whilst running GarbageCollector", e);
        }

        LOGGER.info("GarbageCollectorLambda lambda finished at {}", LocalDateTime.now());
    }
}
