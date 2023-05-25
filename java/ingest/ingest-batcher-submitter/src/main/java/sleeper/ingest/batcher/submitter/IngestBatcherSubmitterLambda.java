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

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.ingest.batcher.IngestBatcherStore;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStore;

import java.io.IOException;
import java.time.Instant;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;

public class IngestBatcherSubmitterLambda implements RequestHandler<SQSEvent, Void> {
    private final IngestBatcherStore store;

    public IngestBatcherSubmitterLambda() throws IOException {
        this(initialiseStore());
    }

    public IngestBatcherSubmitterLambda(IngestBatcherStore store) {
        this.store = store;
    }

    @Override
    public Void handleRequest(SQSEvent input, Context context) {
        return null;
    }

    public void handleMessage(String json, Instant receivedTime) {
        store.addFile(FileIngestRequestSerDe.fromJson(json, receivedTime));
    }

    private static IngestBatcherStore initialiseStore() throws IOException {
        String s3Bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        if (null == s3Bucket) {
            throw new IllegalArgumentException("Couldn't get S3 bucket from environment variable");
        }
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3, s3Bucket);
        return new DynamoDBIngestBatcherStore(AmazonDynamoDBClientBuilder.defaultClient(),
                instanceProperties, new TablePropertiesProvider(s3, instanceProperties));
    }
}
