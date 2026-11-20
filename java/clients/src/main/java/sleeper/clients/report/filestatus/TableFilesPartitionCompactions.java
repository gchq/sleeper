/*
 * Copyright 2022-2026 Crown Copyright
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

import sleeper.core.partition.PartitionTree;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.FileReference;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static java.util.stream.Collectors.groupingBy;

/**
 * Information in the state store about compactions that are currently assigned to files in a partition.
 */
public class TableFilesPartitionCompactions {

    private final String partitionId;
    private final List<TableFilesCompaction> jobs;

    private TableFilesPartitionCompactions(String partitionId, List<TableFilesCompaction> jobs) {
        this.partitionId = partitionId;
        this.jobs = jobs;
    }

    /**
     * Finds information about compactions for a Sleeper table.
     *
     * @param  files      the files in the state store
     * @param  partitions the partition tree
     * @return            the compactions for each partition
     */
    public static List<TableFilesPartitionCompactions> from(AllReferencesToAllFiles files, PartitionTree partitions) {
        Map<String, List<FileReference>> partitionIdToFiles = files.streamFileReferences().collect(groupingBy(FileReference::getPartitionId));
        return partitions.traverseLeavesFirst()
                .map(partition -> TableFilesPartitionCompactions.from(partition.getId(), partitionIdToFiles.getOrDefault(partition.getId(), List.of())))
                .filter(compactions -> !compactions.getJobs().isEmpty())
                .toList();
    }

    /**
     * Finds information about compactions assigned in a partition.
     *
     * @param  partitionId    the partition ID
     * @param  fileReferences the file references in that partition
     * @return                the compactions assigned in the partition
     */
    public static TableFilesPartitionCompactions from(String partitionId, List<FileReference> fileReferences) {
        Map<String, List<FileReference>> jobIdToFiles = fileReferences.stream()
                .filter(reference -> reference.getJobId() != null)
                .collect(groupingBy(FileReference::getJobId));
        Set<String> jobIds = new TreeSet<>(jobIdToFiles.keySet());
        return new TableFilesPartitionCompactions(partitionId, jobIds.stream()
                .map(jobId -> new TableFilesCompaction(jobId, jobIdToFiles.get(jobId)))
                .toList());
    }

    public String getPartitionId() {
        return partitionId;
    }

    public List<TableFilesCompaction> getJobs() {
        return jobs;
    }

}
