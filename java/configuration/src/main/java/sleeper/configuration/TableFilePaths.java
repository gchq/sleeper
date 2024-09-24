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

package sleeper.configuration;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

public class TableFilePaths {
    private TableFilePaths() {
    }

    public static String buildDataFilePathPrefix(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return instanceProperties.get(FILE_SYSTEM) + instanceProperties.get(DATA_BUCKET) + "/" + tableProperties.get(TABLE_ID);
    }

    public static String constructPartitionParquetFilePath(String filePathPrefix, Partition partition, String fileName) {
        return constructPartitionParquetFilePath(filePathPrefix, partition.getId(), fileName);
    }

    public static String constructPartitionParquetFilePath(String filePathPrefix, String partitionId, String fileName) {
        return String.format("%s/data/partition_%s/%s.parquet", filePathPrefix, partitionId, fileName);
    }

    public static String constructQuantileSketchesFilePath(String filePathPrefix, Partition partition, String fileName) {
        return String.format("%s/data/partition_%s/%s.sketches", filePathPrefix, partition.getId(), fileName);
    }
}
