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
package sleeper.core.statestore.transactionlog;

import sleeper.core.partition.Partition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class StateStorePartitions {

    private final Map<String, Partition> partitionById = new HashMap<>();

    public Collection<Partition> all() {
        return partitionById.values();
    }

    public void clear() {
        partitionById.clear();
    }

    public void put(Partition partition) {
        partitionById.put(partition.getId(), partition);
    }

    public Optional<Partition> byId(String id) {
        return Optional.ofNullable(partitionById.get(id));
    }

}
