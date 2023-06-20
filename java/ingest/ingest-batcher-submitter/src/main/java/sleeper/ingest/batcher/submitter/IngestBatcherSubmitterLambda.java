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
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
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
        List<FileIngestRequest> requests;
        try {
            requests = FileIngestRequestSerDe.fromJson(json, receivedTime);
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
            storeFiles(request, receivedTime);
        });
    }

    private void storeFiles(FileIngestRequest request, Instant receivedTime) {
        String bucketName = getBucketName(request);
        String filePath = getFilePath(request);
        ListObjectsV2Result result = s3Client.listObjectsV2(new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(filePath)
                .withDelimiter("/"));
        result.getObjectSummaries().stream()
                .map(summary -> toRequest(summary, bucketName, request.getTableName(), receivedTime))
                .forEach(store::addFile);
        if (result.getObjectSummaries() != null && result.getObjectSummaries().stream()
                .noneMatch(summary -> summary.getKey().equals(getFilePath(request)))) {
            result.getCommonPrefixes().forEach(directory ->
                    streamFilesInDirectory(s3Client, bucketName, directory)
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
        return s3Client.listObjectsV2(new ListObjectsV2Request()
                        .withBucketName(bucketName)
                        .withPrefix(filePath)
                        .withDelimiter("/"))
                .getObjectSummaries().stream();
    }

    private static String getBucketName(FileIngestRequest request) {
        return request.getFile().substring(0, request.getFile().indexOf('/'));
    }

    private static String getFilePath(FileIngestRequest request) {
        return request.getFile().substring(request.getFile().indexOf('/') + 1);
    }
}
