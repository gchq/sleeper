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
package sleeper.compaction.strategy;

import sleeper.core.partition.Partition;
import sleeper.core.statestore.FileReference;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CompactionStrategyIndex {
    private final Map<String, List<FileReference>> filesWithJobIdByPartitionId;
    private final Map<String, List<FileReference>> filesWithNoJobIdByPartitionId;
    private final List<String> leafPartitionIds;

    public CompactionStrategyIndex(List<FileReference> allFileReferences, List<Partition> allPartitions) {
        this.leafPartitionIds = allPartitions.stream()
                .filter(Partition::isLeafPartition)
                .map(Partition::getId)
                .collect(Collectors.toList());
        this.filesWithJobIdByPartitionId = allFileReferences.stream()
                .filter(file -> file.getJobId() != null)
                .collect(Collectors.groupingBy(FileReference::getPartitionId));
        this.filesWithNoJobIdByPartitionId = allFileReferences.stream()
                .filter(file -> file.getJobId() == null)
                .sorted(Comparator.comparingLong(FileReference::getNumberOfRecords))
                .collect(Collectors.groupingBy(FileReference::getPartitionId));
    }

    public List<FileReference> getFilesWithJobIdInPartition(String partitionId) {
        return filesWithJobIdByPartitionId.getOrDefault(partitionId, List.of());
    }

    public List<FileReference> getFilesWithNoJobIdInPartition(String partitionId) {
        return filesWithNoJobIdByPartitionId.getOrDefault(partitionId, List.of());
    }

    public List<String> getLeafPartitionIds() {
        return leafPartitionIds;
    }

}
