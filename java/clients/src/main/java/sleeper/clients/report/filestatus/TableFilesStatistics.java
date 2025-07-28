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

package sleeper.clients.report.filestatus;

import sleeper.core.partition.Partition;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.FileReference;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableFilesStatistics {
    private final Rows rows;
    private final References fileReferences;

    private TableFilesStatistics(Rows rows, References fileReferences) {
        this.rows = rows;
        this.fileReferences = fileReferences;
    }

    public static TableFilesStatistics from(AllReferencesToAllFiles files, Map<String, Partition> partitionById) {

        List<FileReference> fileReferences = files.getFilesWithReferences().stream()
                .flatMap(file -> file.getReferences().stream())
                .collect(Collectors.toUnmodifiableList());
        List<FileReference> fileReferencesInLeafPartitions = fileReferences.stream()
                .filter(f -> partitionById.containsKey(f.getPartitionId()))
                .filter(f -> partitionById.get(f.getPartitionId()).isLeafPartition())
                .collect(Collectors.toList());
        List<FileReference> fileReferencesInNonLeafPartitions = fileReferences.stream()
                .filter(f -> partitionById.containsKey(f.getPartitionId()))
                .filter(f -> !partitionById.get(f.getPartitionId()).isLeafPartition())
                .collect(Collectors.toList());

        return new TableFilesStatistics(
                new Rows(
                        FileRowsStats.from(fileReferences),
                        FileRowsStats.from(fileReferencesInLeafPartitions),
                        FileRowsStats.from(fileReferencesInNonLeafPartitions)),
                new References(
                        files.getFiles().size(),
                        fileReferences.size(),
                        FileReferencesStats.from(fileReferencesInLeafPartitions),
                        FileReferencesStats.from(fileReferencesInNonLeafPartitions)));
    }

    public long getReferencesInLeafPartitions() {
        return fileReferences.leafPartitions.getTotalReferences();
    }

    public long getReferencesInNonLeafPartitions() {
        return fileReferences.nonLeafPartitions.getTotalReferences();
    }

    public FileReferencesStats getLeafPartitionFileReferenceStats() {
        return fileReferences.leafPartitions;
    }

    public FileReferencesStats getNonLeafPartitionFileReferenceStats() {
        return fileReferences.nonLeafPartitions;
    }

    public long getTotalRows() {
        return rows.allPartitions.getTotalRows();
    }

    public long getTotalRowsApprox() {
        return rows.allPartitions.getTotalRowsApprox();
    }

    public long getTotalRowsInLeafPartitions() {
        return rows.leafPartitions.getTotalRows();
    }

    public long getTotalRowsInLeafPartitionsApprox() {
        return rows.leafPartitions.getTotalRowsApprox();
    }

    public long getTotalRowsInNonLeafPartitions() {
        return rows.nonLeafPartitions.getTotalRows();
    }

    public long getTotalRowsInNonLeafPartitionsApprox() {
        return rows.nonLeafPartitions.getTotalRowsApprox();
    }

    public int getFileCount() {
        return fileReferences.totalFiles;
    }

    public int getFileReferenceCount() {
        return fileReferences.totalReferences;
    }

    private static class Rows {
        private final FileRowsStats allPartitions;
        private final FileRowsStats leafPartitions;
        private final FileRowsStats nonLeafPartitions;

        private Rows(FileRowsStats allPartitions, FileRowsStats leafPartitions, FileRowsStats nonLeafPartitions) {
            this.allPartitions = allPartitions;
            this.leafPartitions = leafPartitions;
            this.nonLeafPartitions = nonLeafPartitions;
        }
    }

    private static class References {
        private final int totalFiles;
        private final int totalReferences;
        private final FileReferencesStats leafPartitions;
        private final FileReferencesStats nonLeafPartitions;

        private References(int totalFiles, int totalReferences, FileReferencesStats leafPartitions, FileReferencesStats nonLeafPartitions) {
            this.totalFiles = totalFiles;
            this.totalReferences = totalReferences;
            this.leafPartitions = leafPartitions;
            this.nonLeafPartitions = nonLeafPartitions;
        }
    }

}
