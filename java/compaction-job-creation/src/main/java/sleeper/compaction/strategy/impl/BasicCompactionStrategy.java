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
package sleeper.compaction.strategy.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.strategy.CompactionStrategy;
import sleeper.core.partition.Partition;
import sleeper.statestore.FileInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A simple {@link CompactionStrategy} that lists the active files for a partition in increasing order of the number
 * of records they contain, and iterates through this list creating compaction jobs with at most
 * maximumNumberOfFilesToCompact files in each.
 */
public class BasicCompactionStrategy extends AbstractCompactionStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(BasicCompactionStrategy.class);

    public BasicCompactionStrategy() {
        super();
    }

    @Override
    public List<CompactionJob> createCompactionJobs(List<FileInfo> activeFilesWithJobId, List<FileInfo> activeFilesWithNoJobId, List<Partition> allPartitions) {
        // Get list of partition ids from the active files that have no job id,
        // i.e. they are not already involved in a compaction job. This strategy
        // does not take into account whether there are any compaction jobs
        // running for this partition.
        Set<String> partitionIds = activeFilesWithNoJobId.stream()
                .map(FileInfo::getPartitionId)
                .collect(Collectors.toSet());

        // Get map from partition id to partition
        Map<String, Partition> partitionIdToPartition = new HashMap<>();
        for (Partition partition : allPartitions) {
            partitionIdToPartition.put(partition.getId(), partition);
        }

        // Loop through partitions for the active files with no job id
        List<CompactionJob> compactionJobs = new ArrayList<>();
        for (String partitionId : partitionIds) {
            Partition partition = partitionIdToPartition.get(partitionId);
            if (null == partition) {
                throw new RuntimeException("Cannot find partition for partition id " + partitionId);
            }
            if (partition.isLeafPartition()) {
                compactionJobs.addAll(createJobsForLeafPartition(partition, activeFilesWithNoJobId));
            } else {
                compactionJobs.addAll(createJobsForNonLeafPartition(partition, activeFilesWithNoJobId, partitionIdToPartition));
            }
        }

        return compactionJobs;
    }

    @Override
    protected List<CompactionJob> createJobsForLeafPartition(Partition partition, List<FileInfo> fileInfos) {
        List<CompactionJob> compactionJobs = new ArrayList<>();
        List<FileInfo> filesInAscendingOrder = getFilesInAscendingOrder(partition, fileInfos);

        // Iterate through files, creating jobs for batches of maximumNumberOfFilesToCompact files
        List<FileInfo> filesForJob = new ArrayList<>();
        for (FileInfo fileInfo : filesInAscendingOrder) {
            filesForJob.add(fileInfo);
            if (filesForJob.size() >= compactionFilesBatchSize) {
                // Create job for these files
                LOGGER.info("Creating a job to compact {} files in partition {}",
                        filesForJob.size(), partition);
                compactionJobs.add(createCompactionJob(filesForJob, partition.getId(), tableBucket));
                filesForJob.clear();
            }
        }
        return compactionJobs;
    }
}
