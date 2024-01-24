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
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;

/**
 * A utility class that collects information about the status of files within Sleeper
 * and produces a {@link TableFilesStatus} data structure. This is currently used by
 * FileStatusReport implementations that present this data to the user.
 */
public class FileStatusCollector {
    private final StateStore stateStore;

    public FileStatusCollector(StateStore stateStore) {
        this.stateStore = stateStore;
    }

    public TableFilesStatus run(int maxNumberOFilesWithNoReferencesToCount) throws StateStoreException {
        AllReferencesToAllFiles files = stateStore.getAllFileReferencesWithMaxUnreferenced(maxNumberOFilesWithNoReferencesToCount);
        List<Partition> partitions = stateStore.getAllPartitions();

        List<String> leafPartitionIds = partitions.stream()
                .filter(Partition::isLeafPartition)
                .map(Partition::getId)
                .collect(Collectors.toList());
        List<String> nonLeafPartitionIds = partitions.stream()
                .filter(p -> !p.isLeafPartition())
                .map(Partition::getId)
                .collect(Collectors.toList());
        List<FileReference> fileReferencesInLeafPartitions = files.getFileReferences().stream()
                .filter(f -> leafPartitionIds.contains(f.getPartitionId()))
                .collect(Collectors.toList());
        List<FileReference> fileReferencesInNonLeafPartitions = files.getFileReferences().stream()
                .filter(f -> nonLeafPartitionIds.contains(f.getPartitionId()))
                .collect(Collectors.toList());

        long totalRecords = 0L;
        long totalRecordsInLeafPartitions = 0L;
        long totalRecordsApprox = 0L;
        long totalRecordsInLeafPartitionsApprox = 0L;
        for (Partition partition : partitions) {
            List<FileReference> referencesInPartition = files.getFileReferences().stream()
                    .filter(file -> file.getPartitionId().equals(partition.getId()))
                    .collect(Collectors.toUnmodifiableList());
            long knownRecords = getKnownRecords(referencesInPartition);
            long approxRecords = getApproxRecords(referencesInPartition);
            totalRecords += knownRecords + approxRecords;
            totalRecordsApprox += approxRecords;
            if (partition.isLeafPartition()) {
                totalRecordsInLeafPartitions += knownRecords + approxRecords;
                totalRecordsInLeafPartitionsApprox += approxRecords;
            }
        }

        return TableFilesStatus.builder()
                .leafPartitionCount(leafPartitionIds.size())
                .nonLeafPartitionCount(nonLeafPartitionIds.size())
                .moreThanMax(files.isMoreThanMax())
                .activeFilesCount(files.getFileReferences().size())
                .leafPartitionStats(getPartitionStats(fileReferencesInLeafPartitions))
                .nonLeafPartitionStats(getPartitionStats(fileReferencesInNonLeafPartitions))
                .filesWithNoReferences(files.getFilesWithNoReferences())
                .fileReferences(files.getFileReferences())
                .totalRecords(totalRecords)
                .totalRecordsApprox(totalRecordsApprox)
                .totalRecordsInLeafPartitions(totalRecordsInLeafPartitions)
                .totalRecordsInLeafPartitionsApprox(totalRecordsInLeafPartitionsApprox)
                .build();
    }

    private static long getKnownRecords(List<FileReference> files) {
        return files.stream()
                .filter(not(FileReference::isCountApproximate))
                .map(FileReference::getNumberOfRecords)
                .mapToLong(Long::longValue).sum();
    }

    private static long getApproxRecords(List<FileReference> files) {
        return files.stream()
                .filter(FileReference::isCountApproximate)
                .map(FileReference::getNumberOfRecords)
                .mapToLong(Long::longValue).sum();
    }

    private static TableFilesStatus.PartitionStats getPartitionStats(List<FileReference> references) {
        Map<String, Set<String>> partitionIdToFiles = new TreeMap<>();
        references.forEach(reference -> {
            String partitionId = reference.getPartitionId();
            if (!partitionIdToFiles.containsKey(partitionId)) {
                partitionIdToFiles.put(partitionId, new HashSet<>());
            }
            partitionIdToFiles.get(partitionId).add(reference.getFilename());
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
        return new TableFilesStatus.PartitionStats(min, max, average(total, count), references.size());
    }

    private static Double average(int total, int count) {
        if (count == 0) {
            return null;
        } else {
            return total / (double) count;
        }
    }
}
