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
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;

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
