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
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.ingest.batcher.FileIngestRequest;
import sleeper.ingest.batcher.IngestBatcherStore;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStore;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;

public class IngestBatcherSubmitterLambda implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestBatcherSubmitterLambda.class);
    private final AmazonS3 s3Client;
    private final IngestBatcherStore store;
    private final TablePropertiesProvider tablePropertiesProvider;

    public IngestBatcherSubmitterLambda() throws IOException {
        String s3Bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        if (null == s3Bucket) {
            throw new IllegalArgumentException("Couldn't get S3 bucket from environment variable");
        }
        s3Client = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, s3Bucket);

        this.tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        this.store = new DynamoDBIngestBatcherStore(AmazonDynamoDBClientBuilder.defaultClient(),
                instanceProperties, tablePropertiesProvider);
    }

    public IngestBatcherSubmitterLambda(IngestBatcherStore store, TablePropertiesProvider tablePropertiesProvider, AmazonS3 s3Client) {
        this.store = store;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.s3Client = s3Client;
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
        // Table properties are needed to set the expiry time on DynamoDB records in the store.
        // To avoid that failing, we can discard the message here if the table does not exist.
        if (tablePropertiesProvider.getTablePropertiesIfExists(request.getTableName())
                .isEmpty()) {
            LOGGER.warn("Table does not exist for ingest request: {}", json);
            return;
        }
        // always look in s3, that way we dont need fileSizeBytes from json
        storeFiles(request, receivedTime);
    }

    private void storeFiles(FileIngestRequest request, Instant receivedTime) {
        String bucketName = getBucketName(request);
        String filePath = getFilePath(request);
        ObjectListing result = s3Client.listObjects(bucketName, filePath);
        result.getObjectSummaries().stream()
                .map(summary -> toRequest(summary, bucketName, request.getTableName(), receivedTime))
                .forEach(store::addFile);
        if (result.getObjectSummaries() != null && result.getObjectSummaries().stream()
                .noneMatch(summary -> summary.getKey().equals(getFilePath(request)))) {
            List<String> directoriesToCheck = new ArrayList<>(result.getCommonPrefixes());
            directoriesToCheck.forEach(direrctory ->
                    streamFilesInDirectory(s3Client, bucketName, direrctory)
                            .map(summary -> toRequest(summary, bucketName, request.getTableName(), receivedTime))
                            .forEach(store::addFile));
        }
    }

    private static FileIngestRequest toRequest(S3ObjectSummary summary, String bucketName, String tableName, Instant receivedTime) {
        return FileIngestRequest.builder()
                .file(bucketName + "/" + summary.getKey())
                .fileSizeBytes(summary.getSize())
                .tableName(tableName)
                .receivedTime(receivedTime)
                .build();
    }

    private static Stream<S3ObjectSummary> streamFilesInDirectory(AmazonS3 s3Client, String bucketName, String filePath) {
        return s3Client.listObjects(bucketName, filePath)
                .getObjectSummaries().stream();
    }

    private static String getBucketName(FileIngestRequest request) {
        return request.getFile().substring(0, request.getFile().indexOf('/'));
    }

    private static String getFilePath(FileIngestRequest request) {
        return request.getFile().substring(request.getFile().indexOf('/') + 1);
    }
}
