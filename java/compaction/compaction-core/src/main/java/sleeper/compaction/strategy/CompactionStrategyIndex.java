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
import sleeper.core.table.TableStatus;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class CompactionStrategyIndex {
    private final List<FilesInPartition> filesInLeafPartitions;

    public CompactionStrategyIndex(TableStatus tableStatus, List<FileReference> allFileReferences, List<Partition> allPartitions) {
        Map<String, FilesInAscendingOrder> filesWithNoJobIdByPartition = new HashMap<>();
        Map<String, List<FileReference>> filesWithJobIdByPartition = new HashMap<>();
        Set<String> leafPartitionIds = allPartitions.stream()
                .filter(Partition::isLeafPartition)
                .map(Partition::getId)
                .collect(Collectors.toSet());

        allFileReferences.stream()
                .filter(file -> leafPartitionIds.contains(file.getPartitionId()))
                .forEach(file -> {
                    String partitionId = file.getPartitionId();
                    if (file.getJobId() == null) {
                        filesWithNoJobIdByPartition.computeIfAbsent(partitionId, key -> new FilesInAscendingOrder()).add(file);
                    } else {
                        filesWithJobIdByPartition.computeIfAbsent(partitionId, key -> new ArrayList<>()).add(file);
                    }
                });
        this.filesInLeafPartitions = leafPartitionIds.stream()
                .map(partition -> new FilesInPartition(tableStatus, partition,
                        filesWithNoJobIdByPartition.getOrDefault(partition, new FilesInAscendingOrder()).values(),
                        filesWithJobIdByPartition.getOrDefault(partition, List.of())))
                .collect(Collectors.toList());
    }

    public List<FilesInPartition> getFilesInLeafPartitions() {
        return filesInLeafPartitions;
    }

    private static class FilesInAscendingOrder {
        private List<FileReference> files = new ArrayList<>();

        void add(FileReference file) {
            files.add(file);
        }

        List<FileReference> values() {
            return files.stream()
                    .sorted(Comparator.comparing(FileReference::getNumberOfRecords))
                    .collect(Collectors.toList());
        }
    }

    public static class FilesInPartition {
        private final List<FileReference> filesWithJobId;
        private final List<FileReference> filesWithNoJobIdInAscendingOrder;
        private final String partitionId;
        private final TableStatus tableStatus;

        FilesInPartition(TableStatus tableStatus, String partitionId, List<FileReference> filesWithNoJobIdInAscendingOrder, List<FileReference> filesWithJobId) {
            this.tableStatus = tableStatus;
            this.partitionId = partitionId;
            this.filesWithJobId = filesWithJobId;
            this.filesWithNoJobIdInAscendingOrder = filesWithNoJobIdInAscendingOrder;
        }

        public List<FileReference> getFilesWithJobId() {
            return filesWithJobId;
        }

        public List<FileReference> getFilesWithNoJobIdInAscendingOrder() {
            return filesWithNoJobIdInAscendingOrder;
        }

        public String getPartitionId() {
            return partitionId;
        }

        public TableStatus getTableStatus() {
            return tableStatus;
        }

        @Override
        public int hashCode() {
            return Objects.hash(filesWithJobId, filesWithNoJobIdInAscendingOrder, partitionId, tableStatus);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof FilesInPartition)) {
                return false;
            }
            FilesInPartition other = (FilesInPartition) obj;
            return Objects.equals(filesWithJobId, other.filesWithJobId) && Objects.equals(filesWithNoJobIdInAscendingOrder, other.filesWithNoJobIdInAscendingOrder)
                    && Objects.equals(partitionId, other.partitionId) && Objects.equals(tableStatus, other.tableStatus);
        }

        @Override
        public String toString() {
            return "FilesInPartition{filesWithJobId=" + filesWithJobId + ", filesWithNoJobIdInAscendingOrder=" + filesWithNoJobIdInAscendingOrder + ", partitionId=" + partitionId + ", tableStatus="
                    + tableStatus + "}";
        }
    }
}
