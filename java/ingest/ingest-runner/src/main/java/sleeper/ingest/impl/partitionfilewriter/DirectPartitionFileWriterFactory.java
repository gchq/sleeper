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

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.ingest.impl.ParquetConfiguration;

import java.io.IOException;
import java.util.Objects;

import static sleeper.configuration.properties.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;

public class DirectPartitionFileWriterFactory implements PartitionFileWriterFactory {

    private final ParquetConfiguration parquetConfiguration;
    private final String filePathPrefix;

    private DirectPartitionFileWriterFactory(ParquetConfiguration parquetConfiguration, String filePathPrefix) {
        this.parquetConfiguration = Objects.requireNonNull(parquetConfiguration, "parquetWriterConfiguration must not be null");
        this.filePathPrefix = Objects.requireNonNull(filePathPrefix, "filePathPrefix must not be null");
    }

    public static DirectPartitionFileWriterFactory from(ParquetConfiguration configuration, String filePathPrefix) {
        return new DirectPartitionFileWriterFactory(configuration, filePathPrefix);
    }

    public static DirectPartitionFileWriterFactory from(ParquetConfiguration configuration, InstanceProperties instanceProperties,
                                                        TableProperties tableProperties) {
        return from(configuration, instanceProperties.get(FILE_SYSTEM) + tableProperties.get(DATA_BUCKET));
    }

    @Override
    public PartitionFileWriter createPartitionFileWriter(Partition partition) {
        try {
            return new DirectPartitionFileWriter(
                    partition,
                    parquetConfiguration,
                    filePathPrefix);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
