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
package sleeper.statestore.inmemory;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Schema;
import sleeper.statestore.PartitionStore;
import sleeper.statestore.StateStoreException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class FixedPartitionStore implements PartitionStore {

    private final Schema schema;
    private final List<Partition> partitions = new ArrayList<>();

    public FixedPartitionStore(Schema schema) {
        this.schema = schema;
    }

    @Override
    public List<Partition> getAllPartitions() throws StateStoreException {
        return Collections.unmodifiableList(partitions);
    }

    @Override
    public List<Partition> getLeafPartitions() throws StateStoreException {
        return Collections.unmodifiableList(partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toList()));
    }

    @Override
    public void initialise() {
        initialise(new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct());
    }

    @Override
    public void initialise(List<Partition> partitions) {
        if (this.partitions.isEmpty()) {
            this.partitions.addAll(partitions);
        } else {
            throw new UnsupportedOperationException("Cannot reinitialise partitions with FixedPartitionStore");
        }
    }

    @Override
    public void atomicallyUpdatePartitionAndCreateNewOnes(Partition splitPartition, Partition newPartition1, Partition newPartition2) {
        throw new UnsupportedOperationException("Cannot split partitions with FixedPartitionStore");
    }
}
