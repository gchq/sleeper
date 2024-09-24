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
package sleeper.ingest.impl.partitionfilewriter;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import sleeper.configuration.TableFilePaths;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.ingest.impl.ParquetConfiguration;

import java.io.IOException;
import java.net.URI;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

import static sleeper.configuration.properties.instance.AsyncIngestPartitionFileWriterProperty.ASYNC_INGEST_CLIENT_TYPE;
import static sleeper.configuration.properties.instance.AsyncIngestPartitionFileWriterProperty.ASYNC_INGEST_CRT_PART_SIZE_BYTES;
import static sleeper.configuration.properties.instance.AsyncIngestPartitionFileWriterProperty.ASYNC_INGEST_CRT_TARGET_THROUGHPUT_GBPS;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

public class AsyncS3PartitionFileWriterFactory implements PartitionFileWriterFactory {

    private final ParquetConfiguration parquetConfiguration;
    private final String s3BucketName;
    private final TableFilePaths filePaths;
    private final String localWorkingDirectory;
    private final Supplier<String> fileNameGenerator;
    private final S3TransferManager s3TransferManager;
    private final S3AsyncClient s3AsyncClient;
    private final boolean closeS3AsyncClient;

    private AsyncS3PartitionFileWriterFactory(Builder builder) {
        parquetConfiguration = Objects.requireNonNull(builder.parquetConfiguration, "parquetWriterConfiguration must not be null");
        s3BucketName = Objects.requireNonNull(builder.s3BucketName, "s3BucketName must not be null");
        filePaths = Objects.requireNonNull(builder.filePaths, "filePaths must not be null");
        localWorkingDirectory = Objects.requireNonNull(builder.localWorkingDirectory, "localWorkingDirectory must not be null");
        fileNameGenerator = Objects.requireNonNull(builder.fileNameGenerator, "fileNameGenerator must not be null");
        s3AsyncClient = builder.s3AsyncClient;
        closeS3AsyncClient = builder.closeS3AsyncClient;
        if (s3AsyncClient != null) {
            s3TransferManager = S3TransferManager.builder().s3Client(s3AsyncClient).build();
        } else {
            s3TransferManager = S3TransferManager.create();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builderWith(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return builder().s3BucketName(instanceProperties.get(DATA_BUCKET))
                .filePathPrefix(tableProperties.get(TABLE_ID));
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

    @Override
    public PartitionFileWriter createPartitionFileWriter(Partition partition) {
        try {
            return new AsyncS3PartitionFileWriter(
                    partition,
                    parquetConfiguration,
                    s3BucketName, filePaths,
                    s3TransferManager,
                    localWorkingDirectory,
                    fileNameGenerator.get());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (s3AsyncClient != null && closeS3AsyncClient) {
            s3AsyncClient.close();
        }
        s3TransferManager.close();
    }

    public static final class Builder {
        private ParquetConfiguration parquetConfiguration;
        private S3AsyncClient s3AsyncClient;
        private String s3BucketName;
        private TableFilePaths filePaths;
        private String localWorkingDirectory;
        private boolean closeS3AsyncClient;
        private Supplier<String> fileNameGenerator = () -> UUID.randomUUID().toString();

        private Builder() {
        }

        public Builder parquetConfiguration(ParquetConfiguration parquetConfiguration) {
            this.parquetConfiguration = parquetConfiguration;
            return this;
        }

        public Builder s3AsyncClient(S3AsyncClient s3AsyncClient) {
            this.s3AsyncClient = s3AsyncClient;
            return this;
        }

        public Builder s3AsyncClientOrDefaultFromProperties(
                S3AsyncClient s3AsyncClient, InstanceProperties properties) {
            if (s3AsyncClient == null) {
                this.s3AsyncClient = s3AsyncClientFromProperties(properties);
                closeS3AsyncClient = true;
            } else {
                this.s3AsyncClient = s3AsyncClient;
            }
            return this;
        }

        public Builder s3BucketName(String s3BucketName) {
            this.s3BucketName = s3BucketName;
            return this;
        }

        public Builder filePathPrefix(String filePathPrefix) {
            this.filePaths = TableFilePaths.fromPrefix(filePathPrefix);
            return this;
        }

        public Builder localWorkingDirectory(String localWorkingDirectory) {
            this.localWorkingDirectory = localWorkingDirectory;
            return this;
        }

        public Builder fileNameGenerator(Supplier<String> fileNameGenerator) {
            this.fileNameGenerator = fileNameGenerator;
            return this;
        }

        public AsyncS3PartitionFileWriterFactory build() {
            return new AsyncS3PartitionFileWriterFactory(this);
        }
    }
}
