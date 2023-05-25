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
package sleeper.ingest.batcher.submitter;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.ingest.batcher.FileIngestRequest;
import sleeper.ingest.batcher.IngestBatcherStore;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStore;

import java.io.IOException;
import java.time.Instant;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;

public class IngestBatcherSubmitterLambda implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestBatcherSubmitterLambda.class);
    private final IngestBatcherStore store;
    private final TablePropertiesProvider tablePropertiesProvider;

    public IngestBatcherSubmitterLambda() throws IOException {
        String s3Bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        if (null == s3Bucket) {
            throw new IllegalArgumentException("Couldn't get S3 bucket from environment variable");
        }
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3, s3Bucket);

        this.tablePropertiesProvider = new TablePropertiesProvider(s3, instanceProperties);
        this.store = new DynamoDBIngestBatcherStore(AmazonDynamoDBClientBuilder.defaultClient(),
                instanceProperties, tablePropertiesProvider);
    }

    public IngestBatcherSubmitterLambda(IngestBatcherStore store, TablePropertiesProvider tablePropertiesProvider) {
        this.store = store;
        this.tablePropertiesProvider = tablePropertiesProvider;
    }

    @Override
    public Void handleRequest(SQSEvent input, Context context) {
        input.getRecords().forEach(message ->
                handleMessage(message.getBody(), Instant.now()));
        return null;
    }

    public void handleMessage(String json, Instant receivedTime) {
        FileIngestRequest request;
        try {
            request = FileIngestRequestSerDe.fromJson(json, receivedTime);
        } catch (RuntimeException e) {
            LOGGER.warn("Received invalid ingest request: {}", json, e);
            return;
        }
        if (tablePropertiesProvider.getTablePropertiesIfExists(request.getTableName())
                .isEmpty()) {
            LOGGER.warn("Table does not exist for ingest request: {}", json);
            return;
        }
        store.addFile(request);
    }
}
