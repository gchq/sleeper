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

import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

/**
 * Generator for paths to Sleeper table data files in the data bucket.
 */
public class TableFilePaths {

    private final String filePathPrefix;

    private TableFilePaths(String filePathPrefix) {
        this.filePathPrefix = filePathPrefix;
    }

    /**
     * Creates an instance of this class with the given prefix for paths. Paths will be generated assuming that this
     * prefix puts you at the base of the data bucket.
     *
     * @param  prefix the path prefix
     * @return        an instance of this class
     */
    public static TableFilePaths fromPrefix(String prefix) {
        return new TableFilePaths(prefix);
    }

    /**
     * Creates an instance of this class to generate full paths as they will be held in references to files in the state
     * store.
     *
     * @param  instanceProperties the instance properties
     * @param  tableProperties    the table properties
     * @return                    an instance of this class
     */
    public static TableFilePaths buildDataFilePathPrefix(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return new TableFilePaths(instanceProperties.get(FILE_SYSTEM) + instanceProperties.get(DATA_BUCKET) + "/" + tableProperties.get(TABLE_ID));
    }

    /**
     * Generates a path to a data file in the given partition. Will have `.parquet` appended at the end. The filename
     * must match the filename used for the corresponding sketches file.
     *
     * @param  partition the partition
     * @param  fileName  the filename without file type
     * @return           the file path
     */
    public String constructPartitionParquetFilePath(Partition partition, String fileName) {
        return constructPartitionParquetFilePath(partition.getId(), fileName);
    }

    /**
     * Generates a path to a data file in the given partition. Will have `.parquet` appended at the end. The filename
     * must match the filename used for the corresponding sketches file.
     *
     * @param  partitionId the partition ID
     * @param  fileName    the filename without file type
     * @return             the file path
     */
    public String constructPartitionParquetFilePath(String partitionId, String fileName) {
        return String.format("%s/data/partition_%s/%s.parquet", filePathPrefix, partitionId, fileName);
    }

    /**
     * Generates a path to a sketches file in the given partition. Will have `.sketches` appended at the end. The
     * filename must match the filename used for the corresponding data file.
     *
     * @param  partition the partition
     * @param  fileName  the filename without file type
     * @return           the file path
     */
    public String constructQuantileSketchesFilePath(Partition partition, String fileName) {
        return String.format("%s/data/partition_%s/%s.sketches", filePathPrefix, partition.getId(), fileName);
    }

    public String getFilePathPrefix() {
        return filePathPrefix;
    }
}
