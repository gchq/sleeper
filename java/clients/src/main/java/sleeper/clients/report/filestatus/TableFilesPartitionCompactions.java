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
    private final String partitionLocation;
    private final List<TableFilesCompaction> jobs;

    private TableFilesPartitionCompactions(String partitionId, String partitionLocation, List<TableFilesCompaction> jobs) {
        this.partitionId = partitionId;
        this.partitionLocation = partitionLocation;
        this.jobs = jobs;
    }

    /**
     * Finds information about compactions for a Sleeper table.
     *
     * @param  files      the files in the state store
     * @param  partitions the partition tree
     * @param  jobTracker the job tracker lookup
     * @return            the compactions for each partition
     */
    public static List<TableFilesPartitionCompactions> from(AllReferencesToAllFiles files, PartitionTree partitions, CompactionJobTrackerLookup jobTracker) {
        Map<String, List<FileReference>> partitionIdToFiles = files.streamFileReferences().collect(groupingBy(FileReference::getPartitionId));
        return partitions.traverseLeavesFirst()
                .map(partition -> TableFilesPartitionCompactions.from(partition.getId(), partitions.buildLocationName(partition), partitionIdToFiles.getOrDefault(partition.getId(), List.of()), jobTracker))
                .filter(compactions -> !compactions.getJobs().isEmpty())
                .toList();
    }

    /**
     * Finds information about compactions assigned in a partition.
     *
     * @param  partitionId       the partition ID
     * @param  partitionLocation the name of the partition's location in the partition tree
     * @param  fileReferences    the file references in that partition
     * @param  jobTracker        the job tracker lookup
     * @return                   the compactions assigned in the partition
     */
    public static TableFilesPartitionCompactions from(String partitionId, String partitionLocation, List<FileReference> fileReferences, CompactionJobTrackerLookup jobTracker) {
        Map<String, List<FileReference>> jobIdToFiles = fileReferences.stream()
                .filter(reference -> reference.getJobId() != null)
                .collect(groupingBy(FileReference::getJobId));
        Set<String> jobIds = new TreeSet<>(jobIdToFiles.keySet());
        return new TableFilesPartitionCompactions(partitionId, partitionLocation, jobIds.stream()
                .map(jobId -> TableFilesCompaction.from(jobId, jobIdToFiles.get(jobId), jobTracker))
                .toList());
    }

    public String getPartitionId() {
        return partitionId;
    }

    public String getPartitionLocation() {
        return partitionLocation;
    }

    public List<TableFilesCompaction> getJobs() {
        return jobs;
    }

}
