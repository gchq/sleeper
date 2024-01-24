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

package sleeper.clients.status.report.filestatus;

import sleeper.core.partition.Partition;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.FileReference;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableFilesStatistics {
    private final Records records;
    private final References fileReferences;

    private TableFilesStatistics(Records records, References fileReferences) {
        this.records = records;
        this.fileReferences = fileReferences;
    }

    public static TableFilesStatistics from(AllReferencesToAllFiles files, Map<String, Partition> partitionById) {

        List<FileReference> fileReferencesInLeafPartitions = files.getFileReferences().stream()
                .filter(f -> partitionById.get(f.getPartitionId()).isLeafPartition())
                .collect(Collectors.toList());
        List<FileReference> fileReferencesInNonLeafPartitions = files.getFileReferences().stream()
                .filter(f -> !partitionById.get(f.getPartitionId()).isLeafPartition())
                .collect(Collectors.toList());

        return new TableFilesStatistics(
                new Records(
                        FileRecordsStats.from(files.getFileReferences()),
                        FileRecordsStats.from(fileReferencesInLeafPartitions),
                        FileRecordsStats.from(fileReferencesInNonLeafPartitions)),
                new References(
                        files.getFiles().size(),
                        files.getFileReferences().size(),
                        FileReferenceStats.from(fileReferencesInLeafPartitions),
                        FileReferenceStats.from(fileReferencesInNonLeafPartitions)));
    }

    public long getReferencesInLeafPartitions() {
        return fileReferences.leafPartitions.getTotalReferences();
    }

    public long getReferencesInNonLeafPartitions() {
        return fileReferences.nonLeafPartitions.getTotalReferences();
    }

    public FileReferenceStats getLeafPartitionFileReferenceStats() {
        return fileReferences.leafPartitions;
    }

    public FileReferenceStats getNonLeafPartitionFileReferenceStats() {
        return fileReferences.nonLeafPartitions;
    }

    public long getTotalRecords() {
        return records.allPartitions.getTotalRecords();
    }

    public long getTotalRecordsApprox() {
        return records.allPartitions.getTotalRecordsApprox();
    }

    public long getTotalRecordsInLeafPartitions() {
        return records.leafPartitions.getTotalRecords();
    }

    public long getTotalRecordsInLeafPartitionsApprox() {
        return records.leafPartitions.getTotalRecordsApprox();
    }

    public long getTotalRecordsInNonLeafPartitions() {
        return records.nonLeafPartitions.getTotalRecords();
    }

    public long getTotalRecordsInNonLeafPartitionsApprox() {
        return records.nonLeafPartitions.getTotalRecordsApprox();
    }

    public int getFileCount() {
        return fileReferences.totalFiles;
    }

    public int getFileReferenceCount() {
        return fileReferences.totalReferences;
    }

    private static class Records {
        private final FileRecordsStats allPartitions;
        private final FileRecordsStats leafPartitions;
        private final FileRecordsStats nonLeafPartitions;

        private Records(FileRecordsStats allPartitions, FileRecordsStats leafPartitions, FileRecordsStats nonLeafPartitions) {
            this.allPartitions = allPartitions;
            this.leafPartitions = leafPartitions;
            this.nonLeafPartitions = nonLeafPartitions;
        }
    }

    private static class References {
        private final int totalFiles;
        private final int totalReferences;
        private final FileReferenceStats leafPartitions;
        private final FileReferenceStats nonLeafPartitions;

        private References(int totalFiles, int totalReferences, FileReferenceStats leafPartitions, FileReferenceStats nonLeafPartitions) {
            this.totalFiles = totalFiles;
            this.totalReferences = totalReferences;
            this.leafPartitions = leafPartitions;
            this.nonLeafPartitions = nonLeafPartitions;
        }
    }

}
