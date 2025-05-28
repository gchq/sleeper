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
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.table.TableFilePaths;
import sleeper.ingest.runner.impl.ParquetConfiguration;
import sleeper.sketchesv2.store.SketchesStore;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

public class DirectPartitionFileWriterFactory implements PartitionFileWriterFactory {

    private final ParquetConfiguration parquetConfiguration;
    private final TableFilePaths filePaths;
    private final Supplier<String> fileNameGenerator;
    private final SketchesStore sketchesStore;

    private DirectPartitionFileWriterFactory(
            ParquetConfiguration parquetConfiguration, TableFilePaths filePaths,
            Supplier<String> fileNameGenerator, SketchesStore sketchesStore) {
        this.parquetConfiguration = Objects.requireNonNull(parquetConfiguration, "parquetWriterConfiguration must not be null");
        this.filePaths = Objects.requireNonNull(filePaths, "filePaths must not be null");
        this.fileNameGenerator = Objects.requireNonNull(fileNameGenerator, "fileNameGenerator must not be null");
        this.sketchesStore = Objects.requireNonNull(sketchesStore, "sketchesStore must not be null");
    }

    public static DirectPartitionFileWriterFactory from(ParquetConfiguration configuration, String filePathPrefix, SketchesStore sketchesStore) {
        return from(configuration, filePathPrefix, sketchesStore, () -> UUID.randomUUID().toString());
    }

    public static DirectPartitionFileWriterFactory from(
            ParquetConfiguration configuration, String filePathPrefix, SketchesStore sketchesStore,
            Supplier<String> fileNameGenerator) {
        return new DirectPartitionFileWriterFactory(configuration, TableFilePaths.fromPrefix(filePathPrefix), fileNameGenerator, sketchesStore);
    }

    public static DirectPartitionFileWriterFactory from(
            ParquetConfiguration configuration,
            InstanceProperties instanceProperties,
            TableProperties tableProperties,
            SketchesStore sketchesStore) {
        return from(configuration, instanceProperties, tableProperties, sketchesStore, () -> UUID.randomUUID().toString());
    }

    public static DirectPartitionFileWriterFactory from(
            ParquetConfiguration configuration,
            InstanceProperties instanceProperties,
            TableProperties tableProperties,
            SketchesStore sketchesStore,
            Supplier<String> fileNameGenerator) {
        return from(configuration,
                instanceProperties.get(FILE_SYSTEM) + instanceProperties.get(DATA_BUCKET) +
                        "/" + tableProperties.get(TABLE_ID),
                sketchesStore,
                fileNameGenerator);
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
}
