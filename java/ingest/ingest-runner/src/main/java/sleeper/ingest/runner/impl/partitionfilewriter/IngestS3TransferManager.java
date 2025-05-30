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
package sleeper.ingest.runner.impl.partitionfilewriter;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import sleeper.core.properties.instance.InstanceProperties;

import java.net.URI;
import java.util.Locale;

import static sleeper.core.properties.instance.AsyncIngestPartitionFileWriterProperty.ASYNC_INGEST_CLIENT_TYPE;
import static sleeper.core.properties.instance.AsyncIngestPartitionFileWriterProperty.ASYNC_INGEST_CRT_PART_SIZE_BYTES;
import static sleeper.core.properties.instance.AsyncIngestPartitionFileWriterProperty.ASYNC_INGEST_CRT_TARGET_THROUGHPUT_GBPS;

public class IngestS3TransferManager implements AutoCloseable {

    private final S3TransferManager s3TransferManager;
    private final S3AsyncClient closeS3AsyncClient;

    private IngestS3TransferManager(S3TransferManager s3TransferManager, S3AsyncClient s3AsyncClient) {
        this.s3TransferManager = s3TransferManager;
        this.closeS3AsyncClient = s3AsyncClient;
    }

    public S3TransferManager get() {
        return s3TransferManager;
    }

    @Override
    public void close() {
        if (closeS3AsyncClient != null) {
            closeS3AsyncClient.close();
        }
        s3TransferManager.close();
    }

    public static IngestS3TransferManager create() {
        return new IngestS3TransferManager(S3TransferManager.create(), null);
    }

    public static IngestS3TransferManager fromClient(S3AsyncClient s3AsyncClient) {
        return new IngestS3TransferManager(S3TransferManager.builder().s3Client(s3AsyncClient).build(), null);
    }

    public static IngestS3TransferManager wrap(S3TransferManager s3TransferManager) {
        return new IngestS3TransferManager(s3TransferManager, null);
    }

    public static IngestS3TransferManager s3AsyncClientOrDefaultFromProperties(
            S3AsyncClient s3AsyncClient, InstanceProperties properties) {
        if (s3AsyncClient == null) {
            return fromProperties(properties);
        } else {
            return fromClient(s3AsyncClient);
        }
    }

    public static IngestS3TransferManager fromProperties(InstanceProperties properties) {
        S3AsyncClient s3AsyncClient = s3AsyncClientFromProperties(properties);
        return new IngestS3TransferManager(S3TransferManager.builder().s3Client(s3AsyncClient).build(), s3AsyncClient);
    }

    public static S3AsyncClient s3AsyncClientFromProperties(InstanceProperties properties) {
        String clientType = properties.get(ASYNC_INGEST_CLIENT_TYPE).toLowerCase(Locale.ROOT);
        if ("java".equals(clientType)) {
            return buildS3Client(S3AsyncClient.builder());
        } else if ("crt".equals(clientType)) {
            return buildCrtClient(S3AsyncClient.crtBuilder()
                    .minimumPartSizeInBytes(properties.getLong(ASYNC_INGEST_CRT_PART_SIZE_BYTES))
                    .targetThroughputInGbps(properties.getDouble(ASYNC_INGEST_CRT_TARGET_THROUGHPUT_GBPS)));
        } else {
            throw new IllegalArgumentException("Unrecognised async client type: " + clientType);
        }
    }

    private static S3AsyncClient buildCrtClient(S3CrtAsyncClientBuilder builder) {
        URI customEndpoint = getCustomEndpoint();
        if (customEndpoint != null) {
            return builder
                    .endpointOverride(customEndpoint)
                    .region(Region.US_EAST_1)
                    .forcePathStyle(true)
                    .build();
        } else {
            return builder.build();
        }
    }

    private static S3AsyncClient buildS3Client(S3AsyncClientBuilder builder) {
        URI customEndpoint = getCustomEndpoint();
        if (customEndpoint != null) {
            return builder
                    .endpointOverride(customEndpoint)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                            "test-access-key", "test-secret-key")))
                    .region(Region.US_EAST_1)
                    .forcePathStyle(true)
                    .build();
        } else {
            return builder.build();
        }
    }

    private static URI getCustomEndpoint() {
        String endpoint = System.getenv("AWS_ENDPOINT_URL");
        if (endpoint != null) {
            return URI.create(endpoint);
        }
        return null;
    }

}
