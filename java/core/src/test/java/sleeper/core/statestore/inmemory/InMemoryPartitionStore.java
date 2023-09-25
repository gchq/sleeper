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
package sleeper.core.statestore.inmemory;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.PartitionStore;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InMemoryPartitionStore implements PartitionStore {

    private List<Partition> partitions = List.of();

    public InMemoryPartitionStore(List<Partition> partitions) {
        initialise(partitions);
    }

    public static PartitionStore withSinglePartition(Schema schema) {
        return new InMemoryPartitionStore(new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct());
    }

    @Override
    public List<Partition> getAllPartitions() {
        return partitions;
    }

    @Override
    public List<Partition> getLeafPartitions() {
        return partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public void initialise() {
        throw new UnsupportedOperationException("Not supported because schema would be required");
    }

    @Override
    public void initialise(List<Partition> partitions) {
        this.partitions = partitions;
    }

    @Override
    public void atomicallyUpdatePartitionAndCreateNewOnes(
            Partition splitPartition, Partition newPartition1, Partition newPartition2) {
        partitions = Stream.concat(
                        partitions.stream().filter(partition ->
                                !Objects.equals(partition.getId(), splitPartition.getId())),
                        Stream.of(splitPartition, newPartition1, newPartition2))
                .collect(Collectors.toUnmodifiableList());
    }
}
