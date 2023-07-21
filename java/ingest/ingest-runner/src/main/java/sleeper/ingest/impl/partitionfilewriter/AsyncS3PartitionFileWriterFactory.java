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
package sleeper.ingest.impl.partitionfilewriter;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.ingest.impl.ParquetConfiguration;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static sleeper.configuration.properties.instance.AsyncIngestPartitionFileWriterProperty.ASYNC_INGEST_CLIENT_TYPE;
import static sleeper.configuration.properties.instance.AsyncIngestPartitionFileWriterProperty.ASYNC_INGEST_CRT_PART_SIZE_BYTES;
import static sleeper.configuration.properties.instance.AsyncIngestPartitionFileWriterProperty.ASYNC_INGEST_CRT_TARGET_THROUGHPUT_GBPS;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;

public class AsyncS3PartitionFileWriterFactory implements PartitionFileWriterFactory {

    private final ParquetConfiguration parquetConfiguration;
    private final String s3BucketName;
    private final String localWorkingDirectory;
    private final S3TransferManager s3TransferManager;
    private final S3AsyncClient s3AsyncClient;
    private final boolean closeS3AsyncClient;

    private AsyncS3PartitionFileWriterFactory(Builder builder) {
        parquetConfiguration = Objects.requireNonNull(builder.parquetConfiguration, "parquetWriterConfiguration must not be null");
        s3BucketName = Objects.requireNonNull(builder.s3BucketName, "s3BucketName must not be null");
        localWorkingDirectory = Objects.requireNonNull(builder.localWorkingDirectory, "localWorkingDirectory must not be null");
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

    public static Builder builderWith(TableProperties tableProperties) {
        return builder().tableProperties(tableProperties);
    }

    public static S3AsyncClient s3AsyncClientFromProperties(InstanceProperties properties) {
        String clientType = properties.get(ASYNC_INGEST_CLIENT_TYPE).toLowerCase(Locale.ROOT);
        if ("java".equals(clientType)) {
            return S3AsyncClient.create();
        } else if ("crt".equals(clientType)) {
            return S3AsyncClient.crtBuilder()
                    .minimumPartSizeInBytes(properties.getLong(ASYNC_INGEST_CRT_PART_SIZE_BYTES))
                    .targetThroughputInGbps(properties.getDouble(ASYNC_INGEST_CRT_TARGET_THROUGHPUT_GBPS))
                    .build();
        } else {
            throw new IllegalArgumentException("Unrecognised async client type: " + clientType);
        }
    }

    @Override
    public PartitionFileWriter createPartitionFileWriter(Partition partition) {
        try {
            return new AsyncS3PartitionFileWriter(
                    partition,
                    parquetConfiguration,
                    s3BucketName,
                    s3TransferManager,
                    localWorkingDirectory);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (s3AsyncClient != null && closeS3AsyncClient) {
            s3AsyncClient.close();
        }
        s3TransferManager.close();
    }

    public static final class Builder {
        private ParquetConfiguration parquetConfiguration;
        private S3AsyncClient s3AsyncClient;
        private String s3BucketName;
        private String localWorkingDirectory;
        private boolean closeS3AsyncClient;

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

        public Builder localWorkingDirectory(String localWorkingDirectory) {
            this.localWorkingDirectory = localWorkingDirectory;
            return this;
        }

        public Builder tableProperties(TableProperties tableProperties) {
            return s3BucketName(tableProperties.get(DATA_BUCKET));
        }

        public AsyncS3PartitionFileWriterFactory build() {
            return new AsyncS3PartitionFileWriterFactory(this);
        }
    }
}
