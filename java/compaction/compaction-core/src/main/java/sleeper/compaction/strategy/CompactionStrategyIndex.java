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
    private final List<String> leafPartitionIds;
    private final Map<String, FilesInPartition> filesByPartitionId;

    public CompactionStrategyIndex(List<FileReference> allFileReferences, List<Partition> allPartitions) {
        this.leafPartitionIds = allPartitions.stream()
                .filter(Partition::isLeafPartition)
                .map(Partition::getId)
                .collect(Collectors.toList());
        this.filesByPartitionId = leafPartitionIds.stream()
                .collect(Collectors.toMap(partitionId -> partitionId,
                        partitionId -> FilesInPartition.forPartition(partitionId, allFileReferences)));
    }

    public List<String> getLeafPartitionIds() {
        return leafPartitionIds;
    }

    public FilesInPartition getFilesInPartition(String partitionId) {
        return filesByPartitionId.getOrDefault(partitionId, FilesInPartition.noFiles());
    }

    public static class FilesInPartition {
        private final List<FileReference> filesWithJobId;
        private final List<FileReference> filesWithNoJobIdInAscendingOrder;

        static FilesInPartition noFiles() {
            return new FilesInPartition(List.of(), List.of());
        }

        static FilesInPartition forPartition(String partitionId, List<FileReference> allFileReferences) {
            return new FilesInPartition(
                    allFileReferences.stream()
                            .filter(file -> partitionId.equals(file.getPartitionId()))
                            .filter(file -> file.getJobId() != null)
                            .collect(Collectors.toList()),
                    allFileReferences.stream()
                            .filter(file -> partitionId.equals(file.getPartitionId()))
                            .filter(file -> file.getJobId() == null)
                            .sorted(Comparator.comparing(FileReference::getNumberOfRecords))
                            .collect(Collectors.toList()));
        }

        FilesInPartition(List<FileReference> filesWithJobId, List<FileReference> filesWithNoJobIdInAscendingOrder) {
            this.filesWithJobId = filesWithJobId;
            this.filesWithNoJobIdInAscendingOrder = filesWithNoJobIdInAscendingOrder;
        }

        public List<FileReference> getFilesWithJobId() {
            return filesWithJobId;
        }

        public List<FileReference> getFilesWithNoJobIdInAscendingOrder() {
            return filesWithNoJobIdInAscendingOrder;
        }
    }
}
