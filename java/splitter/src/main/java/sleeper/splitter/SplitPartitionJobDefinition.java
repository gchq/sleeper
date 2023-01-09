/*
 * Copyright 2023 Crown Copyright
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
package sleeper.splitter;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import sleeper.core.partition.Partition;

import java.util.List;

/**
 * A definition of a partition splitting task to be performed.
 */
public class SplitPartitionJobDefinition {
    private final String tableName;
    private final Partition partition;
    private final List<String> fileNames;

    public SplitPartitionJobDefinition(String tableName, Partition partition, List<String> fileNames) {
        this.tableName = tableName;
        this.partition = partition;
        this.fileNames = fileNames;
    }

    public Partition getPartition() {
        return partition;
    }

    public List<String> getFileNames() {
        return fileNames;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(this.partition)
                .append(this.tableName)
                .append(this.fileNames)
                .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final SplitPartitionJobDefinition other = (SplitPartitionJobDefinition) obj;
        return new EqualsBuilder()
                .append(partition, other.partition)
                .append(fileNames, other.fileNames)
                .append(tableName, other.tableName)
                .isEquals();
    }

    @Override
    public String toString() {
        return "SplitPartitionJobDefinition{" + "partition=" + partition + ", fileNames=" + fileNames + '}';
    }

    public String getTableName() {
        return tableName;
    }
}
