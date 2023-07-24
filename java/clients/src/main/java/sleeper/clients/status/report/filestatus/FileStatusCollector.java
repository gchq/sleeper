/*
 * Copyright 2022-2023 Crown Copyright
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
import sleeper.splitter.FindPartitionsToSplit;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * A utility class that collects information about the status of files within Sleeper
 * and produces a {@link FileStatus} data structure. This is currently used by
 * FileStatusReport implementations that present this data to the user.
 */
public class FileStatusCollector {
    private final StateStore stateStore;

    public FileStatusCollector(StateStore stateStore) {
        this.stateStore = stateStore;
    }

    public FileStatus run(int maxNumberOfReadyForGCFilesToCount) throws StateStoreException {
        return run(StateStoreSnapshot.from(stateStore, maxNumberOfReadyForGCFilesToCount));
    }

    public static FileStatus run(StateStoreSnapshot state) throws StateStoreException {
        FileStatus fileStatusReport = new FileStatus();

        StateStoreReadyForGC readyForGC = state.getReadyForGC();
        List<String> leafPartitionIds = state.partitions()
                .filter(Partition::isLeafPartition)
                .map(Partition::getId)
                .collect(Collectors.toList());
        List<String> nonLeafPartitionIds = state.partitions()
                .filter(p -> !p.isLeafPartition())
                .map(Partition::getId)
                .collect(Collectors.toList());
        List<FileInfo> activeFilesInLeafPartitions = state.getFileInPartitionInfosStream()
                .filter(f -> leafPartitionIds.contains(f.getPartitionId()))
                .collect(Collectors.toList());
        List<FileInfo> activeFilesInNonLeafPartitions = state.getFileInPartitionInfosStream()
                .filter(f -> nonLeafPartitionIds.contains(f.getPartitionId()))
                .collect(Collectors.toList());

        fileStatusReport.setLeafPartitionCount(leafPartitionIds.size());
        fileStatusReport.setNonLeafPartitionCount(nonLeafPartitionIds.size());
        fileStatusReport.setReadyForGCFiles(readyForGC.getFiles().size());
        fileStatusReport.setReachedMax(readyForGC.isReachedMax());
        fileStatusReport.setActiveFilesCount(state.fileInPartitionInfosCount());
        fileStatusReport.setActiveFilesInLeafPartitions(activeFilesInLeafPartitions.size());
        fileStatusReport.setActiveFilesInNonLeafPartitions(activeFilesInNonLeafPartitions.size());

        fileStatusReport.setLeafPartitionStats(getPartitionStats(activeFilesInLeafPartitions));
        fileStatusReport.setNonLeafPartitionStats(getPartitionStats(activeFilesInNonLeafPartitions));

        fileStatusReport.setGcFiles(readyForGC.getFiles());
        fileStatusReport.setActiveFiles(state.getFileInPartitionInfos());

        long totalRecords = 0L;
        long totalRecordsInLeafPartitions = 0L;
        for (Partition partition : state.getPartitions()) {
            // TODO This logic simply adds the records in all the file-in-partition records. This will result in counts
            // that are too high as file-in-partition records will be counted twice if the file is in two partitions.
            List<FileInfo> activeFilesInThisPartition = FindPartitionsToSplit.getFilesInPartition(partition, state.getFileInPartitionInfos());
            long numRecordsInPartition = activeFilesInThisPartition.stream().map(FileInfo::getNumberOfRecords).mapToLong(Long::longValue).sum();
            totalRecords += numRecordsInPartition;
            if (partition.isLeafPartition()) {
                totalRecordsInLeafPartitions += numRecordsInPartition;
            }
        }

        fileStatusReport.setTotalRecords(totalRecords);
        fileStatusReport.setTotalRecordsInLeafPartitions(totalRecordsInLeafPartitions);
        return fileStatusReport;
    }

    private static FileStatus.PartitionStats getPartitionStats(List<FileInfo> files) {
        Map<String, Set<String>> partitionIdToFiles = new TreeMap<>();
        files.stream()
                .forEach(file -> {
                    String partitionId = file.getPartitionId();
                    if (!partitionIdToFiles.containsKey(partitionId)) {
                        partitionIdToFiles.put(partitionId, new HashSet<>());
                    }
                    partitionIdToFiles.get(partitionId).add(file.getFilename());
                });
        Integer min = null;
        Integer max = null;
        int total = 0;
        int count = 0;
        for (Map.Entry<String, Set<String>> entry : partitionIdToFiles.entrySet()) {
            int size = entry.getValue().size();
            if (null == min) {
                min = size;
            } else if (size < min) {
                min = size;
            }
            if (null == max) {
                max = size;
            } else if (size > max) {
                max = size;
            }
            total += size;
            count++;
        }
        return new FileStatus.PartitionStats(min, max, average(total, count), files.size());
    }

    private static Double average(int total, int count) {
        if (count == 0) {
            return null;
        } else {
            return total / (double) count;
        }
    }
}
