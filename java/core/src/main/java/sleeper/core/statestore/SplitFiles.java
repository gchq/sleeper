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

package sleeper.core.statestore;

import sleeper.core.partition.Partition;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;
import static sleeper.core.statestore.SplitFileReferenceRequest.splitFileToChildPartitions;

public class SplitFiles {
    private final StateStore stateStore;

    public SplitFiles(StateStore stateStore) {
        this.stateStore = stateStore;
    }

    public static SplitFiles from(StateStore store) {
        return new SplitFiles(store);
    }

    public void split() throws StateStoreException {
        List<FileReference> activeFiles = stateStore.getActiveFiles();
        List<Partition> nonLeafPartitions = stateStore.getAllPartitions().stream()
                .filter(not(Partition::isLeafPartition)).collect(Collectors.toList());
        List<SplitFileReferenceRequest> splitRequests = new ArrayList<>();
        for (Partition partition : nonLeafPartitions) {
            String leftChildPartition = partition.getChildPartitionIds().get(0);
            String rightChildPartition = partition.getChildPartitionIds().get(1);
            activeFiles.stream()
                    .filter(fileReference -> partition.getId().equals(fileReference.getPartitionId()))
                    .map(fileReference -> splitFileToChildPartitions(fileReference, leftChildPartition, rightChildPartition))
                    .forEach(splitRequests::add);
        }
        stateStore.splitFileReferences(splitRequests);
    }
}
