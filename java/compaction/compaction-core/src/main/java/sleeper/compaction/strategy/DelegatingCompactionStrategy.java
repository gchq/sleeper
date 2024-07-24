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
package sleeper.compaction.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.statestore.FileReference;
import sleeper.core.table.TableStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A wrapper containing behaviour common to compaction strategies. Delegates to {@link LeafPartitionCompactionStrategy}
 * and {@link ShouldCreateJobsStrategy}.
 */
public class DelegatingCompactionStrategy implements CompactionStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(DelegatingCompactionStrategy.class);

    private final LeafPartitionCompactionStrategy leafStrategy;
    private final ShouldCreateJobsStrategy shouldCreateJobsStrategy;
    private TableStatus table;

    public DelegatingCompactionStrategy(LeafPartitionCompactionStrategy leafStrategy) {
        this.leafStrategy = leafStrategy;
        this.shouldCreateJobsStrategy = ShouldCreateJobsStrategy.yes();
    }

    public DelegatingCompactionStrategy(
            LeafPartitionCompactionStrategy leafStrategy, ShouldCreateJobsStrategy shouldCreateJobsStrategy) {
        this.leafStrategy = leafStrategy;
        this.shouldCreateJobsStrategy = shouldCreateJobsStrategy;
    }

    @Override
    public void init(InstanceProperties instanceProperties, TableProperties tableProperties, CompactionJobFactory factory) {
        leafStrategy.init(instanceProperties, tableProperties, factory);
        shouldCreateJobsStrategy.init(instanceProperties, tableProperties);
        table = tableProperties.getStatus();
    }

    @Override
    public List<CompactionJob> createCompactionJobs(List<FileReference> activeFilesWithJobId, List<FileReference> activeFilesWithNoJobId, List<Partition> allPartitions) {
        // Get list of partition ids from the above files
        Set<String> partitionIds = activeFilesWithNoJobId.stream()
                .map(FileReference::getPartitionId)
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
                throw new RuntimeException("Cannot find partition for partition id "
                        + partitionId + " in table " + table);
            }

            if (partition.isLeafPartition()) {
                compactionJobs.addAll(createJobsForLeafPartition(partition, activeFilesWithJobId, activeFilesWithNoJobId));
            }
        }

        return compactionJobs;
    }

    private List<CompactionJob> createJobsForLeafPartition(
            Partition partition, List<FileReference> activeFilesWithJobId, List<FileReference> activeFilesWithNoJobId) {

        long maxNumberOfJobsToCreate = shouldCreateJobsStrategy.maxCompactionJobsToCreate(
                partition, activeFilesWithJobId, activeFilesWithNoJobId);
        if (maxNumberOfJobsToCreate < 1) {
            return Collections.emptyList();
        }
        LOGGER.info("Max jobs to create = {}", maxNumberOfJobsToCreate);
        List<CompactionJob> jobs = leafStrategy.createJobsForLeafPartition(partition, activeFilesWithNoJobId);
        LOGGER.info("Defined {} compaction job{} for partition {}, table {}", jobs.size(), 1 == jobs.size() ? "" : "s", partition.getId(), table);
        while (jobs.size() > maxNumberOfJobsToCreate) {
            jobs.remove(jobs.size() - 1);
        }
        LOGGER.info("Created {} compaction job{} for partition {}, table {}", jobs.size(), 1 == jobs.size() ? "" : "s", partition.getId(), table);
        return jobs;
    }
}
