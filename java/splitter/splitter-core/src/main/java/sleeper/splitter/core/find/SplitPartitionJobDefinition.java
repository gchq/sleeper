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
package sleeper.splitter.core.find;

import sleeper.core.partition.Partition;

import java.util.List;
import java.util.Objects;

/**
 * A definition of a partition splitting task to be performed.
 */
public class SplitPartitionJobDefinition {
    private final String tableId;
    private final Partition partition;
    private final List<String> fileNames;

    public SplitPartitionJobDefinition(String tableId, Partition partition, List<String> fileNames) {
        this.tableId = tableId;
        this.partition = partition;
        this.fileNames = fileNames;
    }

    public Partition getPartition() {
        return partition;
    }

    public List<String> getFileNames() {
        return fileNames;
    }

    public String getTableId() {
        return tableId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SplitPartitionJobDefinition that = (SplitPartitionJobDefinition) o;
        return Objects.equals(tableId, that.tableId) && Objects.equals(partition, that.partition) && Objects.equals(fileNames, that.fileNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, partition, fileNames);
    }

    @Override
    public String toString() {
        return "SplitPartitionJobDefinition{" +
                "tableId='" + tableId + '\'' +
                ", partition=" + partition +
                ", fileNames=" + fileNames +
                '}';
    }
}
