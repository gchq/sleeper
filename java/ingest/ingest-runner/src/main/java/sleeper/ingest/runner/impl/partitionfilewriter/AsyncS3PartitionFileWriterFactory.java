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

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.table.TableFilePaths;
import sleeper.ingest.runner.impl.ParquetConfiguration;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

public class AsyncS3PartitionFileWriterFactory implements PartitionFileWriterFactory {

    private final ParquetConfiguration parquetConfiguration;
    private final String s3BucketName;
    private final TableFilePaths filePaths;
    private final String localWorkingDirectory;
    private final Supplier<String> fileNameGenerator;
    private final IngestS3TransferManager s3TransferManager;

    private AsyncS3PartitionFileWriterFactory(Builder builder) {
        parquetConfiguration = Objects.requireNonNull(builder.parquetConfiguration, "parquetWriterConfiguration must not be null");
        s3BucketName = Objects.requireNonNull(builder.s3BucketName, "s3BucketName must not be null");
        filePaths = Objects.requireNonNull(builder.filePaths, "filePaths must not be null");
        localWorkingDirectory = Objects.requireNonNull(builder.localWorkingDirectory, "localWorkingDirectory must not be null");
        fileNameGenerator = Objects.requireNonNull(builder.fileNameGenerator, "fileNameGenerator must not be null");
        s3TransferManager = Optional.ofNullable(builder.s3TransferManager).orElseGet(IngestS3TransferManager::create);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builderWith(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return builder().s3BucketName(instanceProperties.get(DATA_BUCKET))
                .filePathPrefix(tableProperties.get(TABLE_ID));
    }

    @Override
    public PartitionFileWriter createPartitionFileWriter(Partition partition) {
        try {
            return new AsyncS3PartitionFileWriter(
                    partition,
                    parquetConfiguration,
                    s3BucketName, filePaths,
                    s3TransferManager.get(),
                    localWorkingDirectory,
                    fileNameGenerator.get());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        s3TransferManager.close();
    }

    public static final class Builder {
        private ParquetConfiguration parquetConfiguration;
        private IngestS3TransferManager s3TransferManager;
        private String s3BucketName;
        private TableFilePaths filePaths;
        private String localWorkingDirectory;
        private Supplier<String> fileNameGenerator = () -> UUID.randomUUID().toString();

        private Builder() {
        }

        public Builder parquetConfiguration(ParquetConfiguration parquetConfiguration) {
            this.parquetConfiguration = parquetConfiguration;
            return this;
        }

        public Builder s3TransferManager(S3TransferManager s3TransferManager) {
            this.s3TransferManager = IngestS3TransferManager.wrap(s3TransferManager);
            return this;
        }

        public Builder s3AsyncClient(S3AsyncClient s3AsyncClient) {
            this.s3TransferManager = IngestS3TransferManager.fromClient(s3AsyncClient);
            return this;
        }

        public Builder s3AsyncClientOrDefaultFromProperties(
                S3AsyncClient s3AsyncClient, InstanceProperties properties) {
            this.s3TransferManager = IngestS3TransferManager.s3AsyncClientOrDefaultFromProperties(s3AsyncClient, properties);
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
