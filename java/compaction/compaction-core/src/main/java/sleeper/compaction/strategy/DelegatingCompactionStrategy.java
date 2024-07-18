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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A wrapper containing behaviour common to compaction strategies. Delegates to {@link LeafPartitionCompactionStrategy}
 * and {@link ShouldCreateJobsStrategy}.
 */
public class DelegatingCompactionStrategy implements CompactionStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(DelegatingCompactionStrategy.class);

    private final LeafPartitionCompactionStrategy leafStrategy;
    private final ShouldCreateJobsStrategy shouldCreateJobsStrategy;
    private Map<String, List<FileReference>> filesWithJobIdByPartitionId;
    private Map<String, List<FileReference>> filesWithNoJobIdByPartitionId;
    private List<Partition> leafPartitions;
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
    public void init(InstanceProperties instanceProperties, TableProperties tableProperties, List<FileReference> fileReferences, List<Partition> partitions) {
        CompactionJobFactory factory = new CompactionJobFactory(instanceProperties, tableProperties);
        leafStrategy.init(instanceProperties, tableProperties, factory);
        shouldCreateJobsStrategy.init(instanceProperties, tableProperties);
        this.table = tableProperties.getStatus();
        this.leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toList());
        this.filesWithJobIdByPartitionId = fileReferences.stream()
                .filter(file -> file.getJobId() != null)
                .collect(Collectors.groupingBy(FileReference::getPartitionId));
        this.filesWithNoJobIdByPartitionId = fileReferences.stream()
                .filter(file -> file.getJobId() == null)
                .collect(Collectors.groupingBy(FileReference::getPartitionId));
    }

    public List<CompactionJob> createCompactionJobs() {
        // Loop through partitions for the active files with no job id
        List<CompactionJob> compactionJobs = new ArrayList<>();
        for (Partition partition : leafPartitions) {
            compactionJobs.addAll(createJobsForLeafPartition(partition));
        }

        return compactionJobs;
    }

    private List<CompactionJob> createJobsForLeafPartition(Partition partition) {
        return createJobsForLeafPartition(partition,
                filesWithJobIdByPartitionId.getOrDefault(partition.getId(), List.of()),
                filesWithNoJobIdByPartitionId.getOrDefault(partition.getId(), List.of()));
    }

    private List<CompactionJob> createJobsForLeafPartition(
            Partition partition, List<FileReference> activeFilesWithJobId, List<FileReference> activeFilesWithNoJobId) {

        long maxNumberOfJobsToCreate = shouldCreateJobsStrategy.maxCompactionJobsToCreate(partition.getId(), activeFilesWithJobId);
        if (maxNumberOfJobsToCreate < 1) {
            return Collections.emptyList();
        }
        LOGGER.info("Max jobs to create = {}", maxNumberOfJobsToCreate);
        List<CompactionJob> jobs = leafStrategy.createJobsForLeafPartition(partition.getId(), activeFilesWithNoJobId);
        LOGGER.info("Defined {} compaction job{} for partition {}, table {}", jobs.size(), 1 == jobs.size() ? "" : "s", partition.getId(), table);
        while (jobs.size() > maxNumberOfJobsToCreate) {
            jobs.remove(jobs.size() - 1);
        }
        LOGGER.info("Created {} compaction job{} for partition {}, table {}", jobs.size(), 1 == jobs.size() ? "" : "s", partition.getId(), table);
        return jobs;
    }
}
