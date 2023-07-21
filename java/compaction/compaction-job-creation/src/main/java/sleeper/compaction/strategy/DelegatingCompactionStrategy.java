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
package sleeper.compaction.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.schema.Schema;
import sleeper.statestore.FileInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static sleeper.compaction.strategy.impl.CompactionUtils.getFilesInAscendingOrder;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;

/**
 * A {@link CompactionStrategy} that handles splitting compactions for non-leaf partitions,
 * and delegates to {@link LeafPartitionCompactionStrategy} and {@link ShouldCreateJobsStrategy}.
 */
public class DelegatingCompactionStrategy implements CompactionStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(DelegatingCompactionStrategy.class);

    protected final LeafPartitionCompactionStrategy leafStrategy;
    protected final ShouldCreateJobsStrategy shouldCreateJobsStrategy;
    protected CompactionJobFactory factory;
    protected Schema schema;
    protected int compactionFilesBatchSize;

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
    public void init(InstanceProperties instanceProperties, TableProperties tableProperties) {
        factory = new CompactionJobFactory(instanceProperties, tableProperties);
        leafStrategy.init(instanceProperties, tableProperties, factory);
        shouldCreateJobsStrategy.init(instanceProperties, tableProperties);
        schema = tableProperties.getSchema();
        compactionFilesBatchSize = tableProperties.getInt(COMPACTION_FILES_BATCH_SIZE);
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
                compactionJobs.addAll(createJobsForLeafPartition(partition, activeFilesWithJobId, activeFilesWithNoJobId));
            } else {
                compactionJobs.addAll(createJobsForNonLeafPartition(partition, activeFilesWithNoJobId, partitionIdToPartition));
            }
        }

        return compactionJobs;
    }

    private List<CompactionJob> createJobsForLeafPartition(
            Partition partition, List<FileInfo> activeFilesWithJobId, List<FileInfo> activeFilesWithNoJobId) {

        long maxNumberOfJobsToCreate = shouldCreateJobsStrategy.maxCompactionJobsToCreate(
                partition, activeFilesWithJobId, activeFilesWithNoJobId);
        if (maxNumberOfJobsToCreate < 1) {
            return Collections.emptyList();
        }
        LOGGER.info("Max jobs to create = {}", maxNumberOfJobsToCreate);
        List<CompactionJob> jobs = leafStrategy.createJobsForLeafPartition(partition, activeFilesWithNoJobId);
        LOGGER.info("Defined {} compaction job{} for partition {}", jobs.size(), 1 == jobs.size() ? "s" : "", partition.getId());
        while (jobs.size() > maxNumberOfJobsToCreate) {
            jobs.remove(jobs.size() - 1);
        }
        LOGGER.info("Created {} compaction job{} for partition {}", jobs.size(), 1 == jobs.size() ? "s" : "", partition.getId());
        return jobs;
    }

    private List<CompactionJob> createJobsForNonLeafPartition(
            Partition partition, List<FileInfo> fileInfos, Map<String, Partition> partitionIdToPartition) {
        List<CompactionJob> compactionJobs = new ArrayList<>();
        List<FileInfo> filesInAscendingOrder = getFilesInAscendingOrder(partition, fileInfos);

        // Iterate through files, creating jobs for batches of maximumNumberOfFilesToCompact files
        List<FileInfo> filesForJob = new ArrayList<>();
        for (FileInfo fileInfo : filesInAscendingOrder) {
            filesForJob.add(fileInfo);
            if (filesForJob.size() >= compactionFilesBatchSize) {
                // Create job for these files
                LOGGER.info("Creating a job to compact {} files and split into 2 partitions (parent partition is {})",
                        filesForJob.size(), partition);
                compactionJobs.add(createSplittingCompactionJob(partition, partitionIdToPartition, filesForJob));
                filesForJob.clear();
            }
        }

        // If there are any files left (even just 1), create a job for them
        if (!filesForJob.isEmpty()) {
            // Create job for these files
            LOGGER.info("Creating a job to compact {} files in partition {}",
                    filesForJob.size(), partition);
            compactionJobs.add(createSplittingCompactionJob(partition, partitionIdToPartition, filesForJob));
        }
        return compactionJobs;
    }

    private CompactionJob createSplittingCompactionJob(Partition partition, Map<String, Partition> partitionIdToPartition, List<FileInfo> filesForJob) {
        List<String> childPartitions = partitionIdToPartition.get(partition.getId()).getChildPartitionIds();
        Partition leftPartition = partitionIdToPartition.get(childPartitions.get(0));
        Partition rightPartition = partitionIdToPartition.get(childPartitions.get(1));
        Object splitPoint = leftPartition.getRegion()
                .getRange(schema.getRowKeyFieldNames().get(partition.getDimension()))
                .getMax();
        LOGGER.info("Split point is {}", splitPoint);

        return factory.createSplittingCompactionJob(filesForJob,
                partition.getId(),
                leftPartition.getId(),
                rightPartition.getId(),
                splitPoint,
                partition.getDimension());
    }

}
