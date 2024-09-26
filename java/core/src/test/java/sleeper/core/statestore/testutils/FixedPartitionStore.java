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
package sleeper.core.statestore.testutils;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.PartitionStore;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * An in-memory partition store implementation where the partitions are fixed and cannot be changed.
 */
public class FixedPartitionStore implements PartitionStore {

    private final List<Partition> partitions;

    public FixedPartitionStore(Schema schema) {
        this(new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct());
    }

    public FixedPartitionStore(List<Partition> partitions) {
        this.partitions = partitions;
    }

    @Override
    public List<Partition> getAllPartitions() {
        return Collections.unmodifiableList(partitions);
    }

    @Override
    public List<Partition> getLeafPartitions() {
        return partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public void initialise() {
        if (partitions.size() != 1) {
            throw new UnsupportedOperationException(
                    "Called initialise with no parameters when state store fixed with more than one partition");
        }
    }

    @Override
    public void initialise(List<Partition> partitions) {
        if (!this.partitions.equals(partitions)) {
            throw new UnsupportedOperationException("Cannot reinitialise partitions with FixedPartitionStore");
        }
    }

    @Override
    public void atomicallyUpdatePartitionAndCreateNewOnes(Partition splitPartition, Partition newPartition1, Partition newPartition2) {
        throw new UnsupportedOperationException("Cannot split partitions with FixedPartitionStore");
    }

    @Override
    public void clearPartitionData() {
        throw new UnsupportedOperationException("Cannot clear partitions with FixedPartitionStore");
    }
}
