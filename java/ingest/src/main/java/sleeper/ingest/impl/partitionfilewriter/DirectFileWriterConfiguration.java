/*
 * Copyright 2022 Crown Copyright
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

import org.apache.hadoop.conf.Configuration;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;

import java.io.IOException;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.configuration.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.configuration.properties.table.TableProperty.ROW_GROUP_SIZE;

public class DirectFileWriterConfiguration implements FileWriterConfiguration {

    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final Configuration hadoopConfiguration;

    public DirectFileWriterConfiguration(
            InstanceProperties instanceProperties,
            TableProperties tableProperties,
            Configuration hadoopConfiguration) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    @Override
    public PartitionFileWriter createPartitionFileWriter(Partition partition) {
        try {
            return new DirectPartitionFileWriter(
                    tableProperties.getSchema(),
                    partition,
                    tableProperties.getInt(ROW_GROUP_SIZE),
                    tableProperties.getInt(PAGE_SIZE),
                    tableProperties.get(COMPRESSION_CODEC),
                    hadoopConfiguration,
                    instanceProperties.get(FILE_SYSTEM));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
