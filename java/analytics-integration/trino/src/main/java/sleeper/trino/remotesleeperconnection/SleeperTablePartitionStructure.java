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
package sleeper.trino.remotesleeperconnection;

import sleeper.core.partition.Partition;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SleeperTablePartitionStructure {

    private final Instant asOfInstant;
    private final List<Partition> allPartitions;
    private final Map<String, List<String>> partitionToFileMapping;

    SleeperTablePartitionStructure(
            Instant asOfInstant, List<Partition> allPartitions, Map<String, List<String>> partitionToFileMapping) {
        this.asOfInstant = asOfInstant;
        this.allPartitions = allPartitions;
        this.partitionToFileMapping = partitionToFileMapping;
    }

    public Instant getAsOfInstant() {
        return asOfInstant;
    }

    public List<Partition> getAllPartitions() {
        return allPartitions;
    }

    public Map<String, List<String>> getPartitionToFileMapping() {
        return partitionToFileMapping;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SleeperTablePartitionStructure that = (SleeperTablePartitionStructure) o;
        return asOfInstant.equals(that.asOfInstant) && allPartitions.equals(that.allPartitions) && partitionToFileMapping.equals(that.partitionToFileMapping);
    }

    @Override
    public int hashCode() {
        return Objects.hash(asOfInstant, allPartitions, partitionToFileMapping);
    }

    @Override
    public String toString() {
        return "SleeperTablePartitionStructure{" +
                "retrievalInstant=" + asOfInstant +
                ", allPartitions=" + allPartitions +
                ", partitionToFileMapping=" + partitionToFileMapping +
                '}';
    }
}
