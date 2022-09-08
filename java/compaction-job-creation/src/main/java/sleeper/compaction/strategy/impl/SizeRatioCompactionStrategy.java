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
import sleeper.core.partition.Partition;
import sleeper.statestore.FileInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link sleeper.compaction.strategy.CompactionStrategy} that is similar to the strategy used in Accumulo.
 * Given files sorted into increasing size order, it tests whether the sum of the
 * sizes of the files excluding the largest is greater than or equal to a ratio
 * (3 by default) times the size of the largest. If this test is met then a
 * job is created from those files. If the test is not met then the largest file
 * is removed from the list of files and the test is repeated. This continues
 * until either a set of files matching the criteria is found, or there is only
 * one file left.
 * <p>
 * If this results in a group of files of size less than or equal to
 * sleeper.table.compaction.files.batch.size then a compaction job is created
 * for these files. Otherwise the files are split into batches and jobs are
 * created as long as the batch also meets the criteria.
 * <p>
 * The table property sleeper.table.compaction.strategy.sizeratio.max.concurrent.jobs.per.partition
 * controls how many jobs can be running concurrently for each partition.
 */
public class SizeRatioCompactionStrategy extends AbstractCompactionStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(SizeRatioCompactionStrategy.class);

    public SizeRatioCompactionStrategy() {
        super(new SizeRatioLeafStrategy(), new ShouldCreateJobsWithMaxPerPartition());
    }

    @Override
    public List<CompactionJob> createCompactionJobs(List<FileInfo> activeFilesWithJobId, List<FileInfo> activeFilesWithNoJobId, List<Partition> allPartitions) {
        // Get list of partition ids from the above files
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
                long maxNumberOfJobsToCreate = shouldCreateJobsStrategy.maxCompactionJobsToCreate(
                        partition, activeFilesWithJobId, activeFilesWithNoJobId);
                if (maxNumberOfJobsToCreate < 1) {
                    continue;
                }
                LOGGER.info("Max jobs to create = {}", maxNumberOfJobsToCreate);
                List<CompactionJob> jobs = leafStrategy.createJobsForLeafPartition(partition, activeFilesWithNoJobId);
                LOGGER.info("Defined {} compaction job{} for partition {}", jobs.size(), 1 == jobs.size() ? "s" : "", partitionId);
                while (jobs.size() > maxNumberOfJobsToCreate) {
                    jobs.remove(jobs.size() - 1);
                }
                LOGGER.info("Created {} compaction job{} for partition {}", jobs.size(), 1 == jobs.size() ? "s" : "", partitionId);
                compactionJobs.addAll(jobs);
            } else {
                compactionJobs.addAll(createJobsForNonLeafPartition(partition, activeFilesWithNoJobId, partitionIdToPartition));
            }
        }

        return compactionJobs;
    }

}
