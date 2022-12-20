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
import sleeper.core.partition.Partition;
import sleeper.splitter.FindPartitionsToSplit;
import sleeper.statestore.FileInfo;

import java.util.List;

public class PartitionStatus {

    private final Partition partition;
    private final List<FileInfo> filesInPartition;
    private final boolean needsSplitting;

    private PartitionStatus(Partition partition, List<FileInfo> filesInPartition, boolean needsSplitting) {
        this.partition = partition;
        this.filesInPartition = filesInPartition;
        this.needsSplitting = needsSplitting;
    }

    public static PartitionStatus from(
            TableProperties tableProperties, Partition partition, List<FileInfo> activeFiles) {
        List<FileInfo> filesInPartition = FindPartitionsToSplit.getFilesInPartition(partition, activeFiles);
        if (partition.isLeafPartition()) {
            return new PartitionStatus(partition, filesInPartition,
                    FindPartitionsToSplit.partitionNeedsSplitting(tableProperties, partition, filesInPartition));
        } else {
            return new PartitionStatus(partition, filesInPartition, false);
        }
    }

    public boolean isNeedsSplitting() {
        return needsSplitting;
    }

    public boolean isLeafPartition() {
        return partition.isLeafPartition();
    }

    public Partition getPartition() {
        return partition;
    }

    public long getNumberOfRecords() {
        return filesInPartition.stream().mapToLong(FileInfo::getNumberOfRecords).sum();
    }
}
