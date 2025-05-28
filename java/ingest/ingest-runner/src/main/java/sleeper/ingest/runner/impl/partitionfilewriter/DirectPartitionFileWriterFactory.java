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

import sleeper.core.partition.Partition;
import sleeper.core.table.TableFilePaths;
import sleeper.ingest.runner.impl.ParquetConfiguration;
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

    private DirectPartitionFileWriterFactory(Builder builder) {
        this.parquetConfiguration = Objects.requireNonNull(builder.parquetConfiguration, "parquetWriterConfiguration must not be null");
        this.filePaths = Objects.requireNonNull(builder.filePaths, "filePaths must not be null");
        this.fileNameGenerator = Objects.requireNonNull(builder.fileNameGenerator, "fileNameGenerator must not be null");
        this.sketchesStore = Objects.requireNonNull(builder.sketchesStore, "sketchesStore must not be null");
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

    public static final class Builder {
        private ParquetConfiguration parquetConfiguration;
        private TableFilePaths filePaths;
        private Supplier<String> fileNameGenerator = () -> UUID.randomUUID().toString();
        private SketchesStore sketchesStore;

        private Builder() {
        }

        public Builder parquetConfiguration(ParquetConfiguration parquetConfiguration) {
            this.parquetConfiguration = parquetConfiguration;
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
