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
package sleeper.core.statestore.transactionlog.transactions;

import sleeper.core.partition.Partition;
import sleeper.core.statestore.transactionlog.StateStoreState;

import java.util.List;

public class InitialisePartitionsTransaction implements StateStoreTransaction {

    private final List<Partition> partitions;

    public InitialisePartitionsTransaction(List<Partition> partitions) {
        this.partitions = partitions;
    }

    @Override
    public void validate(StateStoreState state) {
    }

    @Override
    public void apply(StateStoreState state) {
        partitions.forEach(partition -> state.partitionById().put(partition.getId(), partition));
    }

}
