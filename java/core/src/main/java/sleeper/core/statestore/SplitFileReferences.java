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

public class SplitFileReferences {
    private final StateStore stateStore;

    public SplitFileReferences(StateStore stateStore) {
        this.stateStore = stateStore;
    }

    public static SplitFileReferences from(StateStore store) {
        return new SplitFileReferences(store);
    }

    public void split() throws StateStoreException {
        List<FileReference> activeFiles = stateStore.getActiveFiles();
        List<Partition> nonLeafPartitions = stateStore.getAllPartitions().stream()
                .filter(not(Partition::isLeafPartition)).collect(Collectors.toList());
        List<SplitFileReferenceRequest> splitRequests = new ArrayList<>();
        for (Partition partition : nonLeafPartitions) {
            activeFiles.stream()
                    .filter(fileReference -> partition.getId().equals(fileReference.getPartitionId()))
                    .map(fileReference -> splitFileInPartition(fileReference, partition))
                    .forEach(splitRequests::add);
        }
        stateStore.splitFileReferences(splitRequests);
    }

    private static SplitFileReferenceRequest splitFileInPartition(FileReference file, Partition partition) {
        return splitFileToChildPartitions(file, partition.getChildPartitionIds().get(0), partition.getChildPartitionIds().get(1));
    }
}
