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

import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.table.TableFilePaths;
import sleeper.ingest.runner.impl.ParquetConfiguration;
import sleeper.sketchesv2.store.LocalFileSystemSketchesStore;
import sleeper.sketchesv2.store.S3SketchesStore;
import sleeper.sketchesv2.store.SketchesStore;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

public class DirectPartitionFileWriterFactory implements PartitionFileWriterFactory {

    private final ParquetConfiguration parquetConfiguration;
    private final TableFilePaths filePaths;
    private final Supplier<String> fileNameGenerator;
    private final SketchesStore sketchesStore;
    private final S3TransferManagerWrapper s3TransferManager;

    private DirectPartitionFileWriterFactory(Builder builder) {
        this.parquetConfiguration = Objects.requireNonNull(builder.parquetConfiguration, "parquetWriterConfiguration must not be null");
        this.filePaths = Objects.requireNonNull(builder.filePaths, "filePaths must not be null");
        this.fileNameGenerator = Objects.requireNonNull(builder.fileNameGenerator, "fileNameGenerator must not be null");
        this.sketchesStore = Objects.requireNonNull(builder.sketchesStore, "sketchesStore must not be null");
        this.s3TransferManager = builder.s3TransferManager;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public PartitionFileWriter createPartitionFileWriter(Partition partition) {
        try {
            return new DirectPartitionFileWriter(
                    partition,
                    parquetConfiguration,
                    filePaths,
                    fileNameGenerator.get(),
                    sketchesStore);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (s3TransferManager != null) {
            s3TransferManager.close();
        }
    }

    public static final class Builder {
        private ParquetConfiguration parquetConfiguration;
        private TableFilePaths filePaths;
        private Supplier<String> fileNameGenerator = () -> UUID.randomUUID().toString();
        private SketchesStore sketchesStore;
        private S3TransferManagerWrapper s3TransferManager;

        private Builder() {
        }

        public Builder parquetConfiguration(ParquetConfiguration parquetConfiguration) {
            this.parquetConfiguration = parquetConfiguration;
            return this;
        }

        public Builder filePathsAndSketchesStoreFromPropertiesAndClientOrDefault(
                InstanceProperties instanceProperties, TableProperties tableProperties, S3AsyncClient s3AsyncClient) {
            filePaths = TableFilePaths.buildDataFilePathPrefix(instanceProperties, tableProperties);
            if (filePaths.getFilePathPrefix().startsWith("s3a://")) {
                s3TransferManager = S3TransferManagerWrapper.s3AsyncClientOrDefaultFromProperties(s3AsyncClient, instanceProperties);
                sketchesStore = S3SketchesStore.createWriteOnly(s3TransferManager.get());
            } else {
                sketchesStore = new LocalFileSystemSketchesStore();
            }
            return this;
        }

        public Builder filePaths(TableFilePaths filePaths) {
            this.filePaths = filePaths;
            return this;
        }

        public Builder sketchesStore(SketchesStore sketchesStore) {
            this.sketchesStore = sketchesStore;
            return this;
        }

        public Builder fileNameGenerator(Supplier<String> fileNameGenerator) {
            this.fileNameGenerator = fileNameGenerator;
            return this;
        }

        public DirectPartitionFileWriterFactory build() {
            return new DirectPartitionFileWriterFactory(this);
        }

    }

}
