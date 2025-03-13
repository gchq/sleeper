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

package sleeper.core.statestore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.Partition;
import sleeper.core.statestore.transactionlog.transaction.impl.SplitFileReferencesTransaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;
import static sleeper.core.statestore.SplitFileReferenceRequest.splitFileToChildPartitions;

/**
 * Updates the state store to push references to all files down the partition tree towards the leaf partitions.
 */
public class SplitFileReferences {
    private static final Logger LOGGER = LoggerFactory.getLogger(SplitFileReferences.class);
    private final StateStore stateStore;

    public SplitFileReferences(StateStore stateStore) {
        this.stateStore = stateStore;
    }

    /**
     * Creates an instance of this class to update the given state store.
     *
     * @param  store the state store
     * @return       the new instance
     */
    public static SplitFileReferences from(StateStore store) {
        return new SplitFileReferences(store);
    }

    /**
     * Performs splits on any file references that need splitting. Updates the state store, and pushes each file one
     * level down the partition tree.
     *
     * @throws StateStoreException thrown if the state store fails to update
     */
    public void split() throws StateStoreException {
        Map<String, List<FileReference>> fileReferencesByPartitionId = stateStore.getFileReferencesWithNoJobId().stream()
                .collect(Collectors.groupingBy(FileReference::getPartitionId));
        List<Partition> nonLeafPartitions = stateStore.getAllPartitions().stream()
                .filter(not(Partition::isLeafPartition)).collect(Collectors.toList());
        List<SplitFileReferenceRequest> splitRequests = new ArrayList<>();
        nonLeafPartitions.stream()
                .flatMap(partition -> fileReferencesByPartitionId.getOrDefault(partition.getId(), List.of()).stream()
                        .map(fileReference -> splitFileInPartition(fileReference, partition)))
                .forEach(splitRequests::add);
        LOGGER.info("Found {} files in non-leaf partitions that need splitting", splitRequests.size());
        new SplitFileReferencesTransaction(splitRequests).synchronousCommit(stateStore);
    }

    private static SplitFileReferenceRequest splitFileInPartition(FileReference file, Partition partition) {
        return splitFileToChildPartitions(file, partition.getChildPartitionIds().get(0), partition.getChildPartitionIds().get(1));
    }
}
