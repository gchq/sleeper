/*
 * Copyright 2022 Crown Copyright
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
package sleeper.status.report.partitions;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.util.List;
import java.util.stream.Collectors;

public class PartitionsStatus {

    private final List<PartitionStatus> partitions;
    private final int numLeafPartitions;

    public PartitionsStatus(List<PartitionStatus> partitions, int numLeafPartitions) {
        this.partitions = partitions;
        this.numLeafPartitions = numLeafPartitions;
    }

    public static PartitionsStatus from(TableProperties tableProperties, StateStore store) throws StateStoreException {
        List<FileInfo> activeFiles = store.getActiveFiles();
        List<PartitionStatus> partitions = store.getAllPartitions().stream()
                .map(partition -> PartitionStatus.from(tableProperties, partition, activeFiles))
                .collect(Collectors.toList());
        int numLeafPartitions = (int) partitions.stream().filter(PartitionStatus::isLeafPartition).count();
        return new PartitionsStatus(partitions, numLeafPartitions);
    }

    public int getNumPartitions() {
        return partitions.size();
    }

    public int getNumLeafPartitions() {
        return numLeafPartitions;
    }

    public long getNumSplittingPartitions() {
        return partitions.stream().filter(PartitionStatus::isNeedsSplitting).count();
    }

    public List<PartitionStatus> getPartitions() {
        return partitions;
    }

}
