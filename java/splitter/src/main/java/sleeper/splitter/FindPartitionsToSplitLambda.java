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
package sleeper.splitter;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import sleeper.table.job.TableLister;
import sleeper.utils.HadoopConfigurationProvider;

import java.io.IOException;
import java.time.LocalDateTime;

import static sleeper.configuration.properties.instance.PartitionSplittingProperty.MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.PARTITION_SPLITTING_QUEUE_URL;

/**
 * This is triggered via a periodic Cloudwatch rule. It runs
 * {@link FindPartitionsToSplit} for each table.
 */
@SuppressWarnings("unused")
public class FindPartitionsToSplitLambda {
    private final AmazonS3 s3Client;
    private final AmazonSQS sqsClient;
    private final InstanceProperties instanceProperties;
    private final StateStoreProvider stateStoreProvider;

    private static final Logger LOGGER = LoggerFactory.getLogger(FindPartitionsToSplitLambda.class);
    private final TablePropertiesProvider tablePropertiesProvider;

    public FindPartitionsToSplitLambda() {
        this.s3Client = AmazonS3ClientBuilder.defaultClient();
        String s3Bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        if (null == s3Bucket) {
            throw new RuntimeException("Couldn't get S3 bucket from environment variable");
        }
        this.instanceProperties = new InstanceProperties();
        this.instanceProperties.loadFromS3(s3Client, s3Bucket);
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        this.sqsClient = AmazonSQSClientBuilder.defaultClient();
        this.stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, HadoopConfigurationProvider.getConfigurationForLambdas(instanceProperties));
        this.tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
    }

    public void eventHandler(ScheduledEvent event, Context context) {
        LOGGER.info("FindPartitionsToSplitLambda triggered at {}", event.getTime());
        new TableLister(s3Client, instanceProperties).listTables().stream().map(tableName -> {
            TableProperties tableProperties = tablePropertiesProvider.getTableProperties(tableName);
            StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
            return new FindPartitionsToSplit(tableName, tablePropertiesProvider, stateStore,
                    instanceProperties.getInt(MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB),
                    sqsClient, instanceProperties.get(PARTITION_SPLITTING_QUEUE_URL));
        }).forEach(partitionsFinder -> {
            try {
                partitionsFinder.run();
            } catch (IOException | StateStoreException e) {
                LOGGER.error("StateStoreException thrown whilst running FindPartitionsToSplit", e);
            }
        });
        LOGGER.info("FindPartitionsToSplitLambda finished at {}", LocalDateTime.now());
    }
}
