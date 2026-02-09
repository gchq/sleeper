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
import sleeper.core.statestore.StateStore;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;

/**
 * Collects information about files in a Sleeper table for reporting. Produces a {@link TableFilesStatus} data
 * structure.
 */
public class FileStatusCollector {
    private final StateStore stateStore;

    public FileStatusCollector(StateStore stateStore) {
        this.stateStore = stateStore;
    }

    /**
     * Retrieves data from the state store and produces a report on the status of files.
     *
     * @param  maxNumberOFilesWithNoReferencesToCount the maximum number of files that have no references to include in
     *                                                the report
     * @return                                        the report
     */
    public TableFilesStatus run(int maxNumberOFilesWithNoReferencesToCount) {
        AllReferencesToAllFiles files = stateStore.getAllFilesWithMaxUnreferenced(maxNumberOFilesWithNoReferencesToCount);
        List<Partition> partitions = stateStore.getAllPartitions();

        int leafPartitionCount = partitions.stream()
                .filter(Partition::isLeafPartition)
                .mapToInt(p -> 1).sum();
        int nonLeafPartitionCount = partitions.size() - leafPartitionCount;
        Map<String, Partition> partitionById = partitions.stream()
                .collect(Collectors.toMap(Partition::getId, identity()));

        return TableFilesStatus.builder()
                .leafPartitionCount(leafPartitionCount)
                .nonLeafPartitionCount(nonLeafPartitionCount)
                .statistics(TableFilesStatistics.from(files, partitionById))
                .files(files)
                .build();
    }

}
