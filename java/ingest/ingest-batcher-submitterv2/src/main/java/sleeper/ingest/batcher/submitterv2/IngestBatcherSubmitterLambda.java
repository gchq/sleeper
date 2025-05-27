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
package sleeper.ingest.batcher.submitterv2;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.properties.S3PropertiesReloader;
import sleeper.configurationv2.properties.S3TableProperties;
import sleeper.configurationv2.table.index.DynamoDBTableIndex;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.table.TableIndex;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequest;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequestSerDe;
import sleeper.ingest.batcher.storev2.DynamoDBIngestBatcherStore;
import sleeper.parquet.utils.HadoopConfigurationProvider;

import java.time.Instant;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

public class IngestBatcherSubmitterLambda implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestBatcherSubmitterLambda.class);
    private final PropertiesReloader propertiesReloader;
    private final IngestBatcherSubmitRequestSerDe serDe = new IngestBatcherSubmitRequestSerDe();
    private final IngestBatcherSubmitter submitter;
    private final IngestBatcherSubmitDeadLetterQueue deadLetterQueue;

    public IngestBatcherSubmitterLambda() {
        String s3Bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        if (null == s3Bucket) {
            throw new IllegalArgumentException("Couldn't get S3 bucket from environment variable");
        }
        S3Client s3Client = S3Client.create();
        SqsClient sqsClient = SqsClient.create();
        DynamoDbClient dynamoDBClient = DynamoDbClient.create();

        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, s3Bucket);

        TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDBClient);
        this.propertiesReloader = S3PropertiesReloader.ifConfigured(s3Client, instanceProperties, tablePropertiesProvider);
        this.deadLetterQueue = new IngestBatcherSubmitDeadLetterQueue(instanceProperties, sqsClient);
        this.submitter = new IngestBatcherSubmitter(instanceProperties,
                HadoopConfigurationProvider.getConfigurationForLambdas(instanceProperties),
                new DynamoDBTableIndex(instanceProperties, dynamoDBClient),
                new DynamoDBIngestBatcherStore(dynamoDBClient, instanceProperties, tablePropertiesProvider),
                deadLetterQueue);
    }

    public IngestBatcherSubmitterLambda(
            IngestBatcherStore store, InstanceProperties instanceProperties,
            TableIndex tableIndex, Configuration conf, IngestBatcherSubmitDeadLetterQueue dlQueue) {
        this.propertiesReloader = PropertiesReloader.neverReload();
        this.deadLetterQueue = dlQueue;
        this.submitter = new IngestBatcherSubmitter(instanceProperties, conf, tableIndex, store, deadLetterQueue);
    }

    @Override
    public Void handleRequest(SQSEvent input, Context context) {
        propertiesReloader.reloadIfNeeded();
        input.getRecords().forEach(message -> handleMessage(message.getBody(), Instant.now()));
        return null;
    }

    public void handleMessage(String json, Instant receivedTime) {
        IngestBatcherSubmitRequest request;
        try {
            request = serDe.fromJson(json);
        } catch (RuntimeException e) {
            LOGGER.warn("Received invalid ingest request: {}", json, e);
            deadLetterQueue.submit(json);
            return;
        }
        submitter.submit(request, receivedTime);
    }
}
