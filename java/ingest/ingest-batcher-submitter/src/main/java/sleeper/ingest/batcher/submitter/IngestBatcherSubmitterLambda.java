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
package sleeper.ingest.batcher.submitter;

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
import sleeper.configuration.properties.instance.S3InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.ingest.batcher.FileIngestRequest;
import sleeper.ingest.batcher.IngestBatcherStore;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStore;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;

import java.time.Instant;
import java.util.List;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

public class IngestBatcherSubmitterLambda implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestBatcherSubmitterLambda.class);
    private final PropertiesReloader propertiesReloader;
    private final IngestBatcherStore store;
    private final FileIngestRequestSerDe fileIngestRequestSerDe;

    public IngestBatcherSubmitterLambda() {
        String s3Bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        if (null == s3Bucket) {
            throw new IllegalArgumentException("Couldn't get S3 bucket from environment variable");
        }
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, s3Bucket);

        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient);
        this.store = new DynamoDBIngestBatcherStore(dynamoDBClient, instanceProperties, tablePropertiesProvider);
        this.propertiesReloader = PropertiesReloader.ifConfigured(s3Client, instanceProperties, tablePropertiesProvider);
        this.fileIngestRequestSerDe = new FileIngestRequestSerDe(instanceProperties,
                HadoopConfigurationProvider.getConfigurationForLambdas(instanceProperties),
                new DynamoDBTableIndex(instanceProperties, dynamoDBClient));
    }

    public IngestBatcherSubmitterLambda(
            IngestBatcherStore store, InstanceProperties instanceProperties,
            TableIndex tableIndex, Configuration conf) {
        this.store = store;
        this.propertiesReloader = PropertiesReloader.neverReload();
        this.fileIngestRequestSerDe = new FileIngestRequestSerDe(instanceProperties, conf, tableIndex);
    }

    @Override
    public Void handleRequest(SQSEvent input, Context context) {
        propertiesReloader.reloadIfNeeded();
        input.getRecords().forEach(message -> handleMessage(message.getBody(), Instant.now()));
        return null;
    }

    public void handleMessage(String json, Instant receivedTime) {
        List<FileIngestRequest> requests;
        try {
            requests = fileIngestRequestSerDe.fromJson(json, receivedTime);
        } catch (RuntimeException e) {
            LOGGER.warn("Received invalid ingest request: {}", json, e);
            return;
        }
        requests.forEach(store::addFile);
    }
}
