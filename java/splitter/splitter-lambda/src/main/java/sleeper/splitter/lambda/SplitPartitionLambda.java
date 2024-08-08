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
package sleeper.splitter.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.PropertiesReloader;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;
import sleeper.splitter.find.SplitPartitionJobDefinition;
import sleeper.splitter.find.SplitPartitionJobDefinitionSerDe;
import sleeper.splitter.split.SplitPartition;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * Triggered by an SQS event containing a partition splitting job to do.
 */
@SuppressWarnings("unused")
public class SplitPartitionLambda implements RequestHandler<SQSEvent, Void> {
    private final PropertiesReloader propertiesReloader;
    private final Configuration conf;
    private static final Logger LOGGER = LoggerFactory.getLogger(SplitPartitionLambda.class);
    private final StateStoreProvider stateStoreProvider;
    private final TablePropertiesProvider tablePropertiesProvider;

    public SplitPartitionLambda() {
        String s3Bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        if (null == s3Bucket) {
            throw new RuntimeException("Couldn't get S3 bucket from environment variable");
        }
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, s3Bucket);

        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        this.conf = HadoopConfigurationProvider.getConfigurationForLambdas(instanceProperties);
        this.tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient);
        this.stateStoreProvider = new StateStoreProvider(instanceProperties, s3Client, dynamoDBClient, conf);
        this.propertiesReloader = PropertiesReloader.ifConfigured(s3Client, instanceProperties, tablePropertiesProvider);
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        propertiesReloader.reloadIfNeeded();
        try {
            for (SQSEvent.SQSMessage message : event.getRecords()) {
                String serialisedJob = message.getBody();
                SplitPartitionJobDefinition job = new SplitPartitionJobDefinitionSerDe(tablePropertiesProvider)
                        .fromJson(serialisedJob);
                LOGGER.info("Received partition splitting job {}", job);
                TableProperties tableProperties = tablePropertiesProvider.getById(job.getTableId());
                StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
                SplitPartition splitPartition = new SplitPartition(stateStore, tableProperties.getSchema(), conf);
                splitPartition.splitPartition(job.getPartition(), job.getFileNames());
            }
        } catch (IOException | StateStoreException ex) {
            LOGGER.error("Exception handling partition splitting job", ex);
        }
        return null;
    }
}
