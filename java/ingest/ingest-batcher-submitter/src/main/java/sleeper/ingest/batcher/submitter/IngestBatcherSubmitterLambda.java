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
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.PropertiesReloader;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.ingest.batcher.FileIngestRequest;
import sleeper.ingest.batcher.IngestBatcherStore;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStore;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;

public class IngestBatcherSubmitterLambda implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestBatcherSubmitterLambda.class);
    private final PropertiesReloader propertiesReloader;
    private final IngestBatcherStore store;
    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final Configuration configuration;

    public IngestBatcherSubmitterLambda() throws IOException {
        String s3Bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        if (null == s3Bucket) {
            throw new IllegalArgumentException("Couldn't get S3 bucket from environment variable");
        }
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, s3Bucket);

        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        this.store = new DynamoDBIngestBatcherStore(AmazonDynamoDBClientBuilder.defaultClient(),
                instanceProperties, tablePropertiesProvider);
        this.configuration = new Configuration();
        this.propertiesReloader = PropertiesReloader.ifConfigured(s3Client, instanceProperties, tablePropertiesProvider);
    }

    public IngestBatcherSubmitterLambda(IngestBatcherStore store, InstanceProperties instanceProperties,
                                        TablePropertiesProvider tablePropertiesProvider, Configuration conf) {
        this.store = store;
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.configuration = conf;
        this.propertiesReloader = PropertiesReloader.neverReload();
    }

    @Override
    public Void handleRequest(SQSEvent input, Context context) {
        propertiesReloader.reloadIfNeeded();
        input.getRecords().forEach(message ->
                handleMessage(message.getBody(), Instant.now()));
        return null;
    }

    public void handleMessage(String json, Instant receivedTime) {
        List<FileIngestRequest> requests;
        try {
            requests = FileIngestRequestSerDe.fromJson(json, instanceProperties, configuration, receivedTime);
        } catch (RuntimeException e) {
            LOGGER.warn("Received invalid ingest request: {}", json, e);
            return;
        }
        // Table properties are needed to set the expiry time on DynamoDB records in the store.
        // To avoid that failing, we can discard the message here if the table does not exist.
        if (requests.size() > 0 &&
                tablePropertiesProvider.getTablePropertiesIfExists(requests.get(0).getTableName()).isEmpty()) {
            LOGGER.warn("Table does not exist for ingest request: {}", json);
            return;
        }
        requests.forEach(request -> {
            LOGGER.info("Adding {} to store", request.getFile());
            store.addFile(request);
        });
    }
}
