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

package sleeper.compaction.job;

import sleeper.configuration.TableUtils;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

public class CompactionOutputFileNameFactory {

    private final String outputFilePrefix;

    public CompactionOutputFileNameFactory(String outputFilePrefix) {
        this.outputFilePrefix = outputFilePrefix;
    }

    public static CompactionOutputFileNameFactory forTable(
            InstanceProperties instanceProperties, TableProperties tableProperties) {
        String outputFilePrefix = TableUtils.buildDataFilePathPrefix(instanceProperties, tableProperties);
        return new CompactionOutputFileNameFactory(outputFilePrefix);
    }

    public String jobPartitionFile(String jobId, String partitionId) {
        return TableUtils.constructPartitionParquetFilePath(outputFilePrefix, partitionId, jobId);
    }

    public String getOutputFilePrefix() {
        return outputFilePrefix;
    }
}
