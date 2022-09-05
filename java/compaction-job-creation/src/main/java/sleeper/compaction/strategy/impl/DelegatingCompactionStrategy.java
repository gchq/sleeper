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
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.schema.Schema;
import sleeper.statestore.FileInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static sleeper.compaction.strategy.impl.CompactionUtils.getFilesInAscendingOrder;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;

public class DelegatingCompactionStrategy implements CompactionStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(DelegatingCompactionStrategy.class);

    private final LeafPartitionCompactionStrategy leafStrategy;
    private final ShouldCreateJobsStrategy shouldCreateJobsStrategy;
    private CompactionFactory factory;
    private Schema schema;
    private int compactionFilesBatchSize;

    public DelegatingCompactionStrategy(LeafPartitionCompactionStrategy leafStrategy) {
        this.leafStrategy = leafStrategy;
        shouldCreateJobsStrategy = ShouldCreateJobsStrategy.yes();
    }

    public DelegatingCompactionStrategy(
            LeafPartitionCompactionStrategy leafStrategy, ShouldCreateJobsStrategy shouldCreateJobsStrategy) {
        this.leafStrategy = leafStrategy;
        this.shouldCreateJobsStrategy = shouldCreateJobsStrategy;
    }

    @Override
    public void init(InstanceProperties instanceProperties, TableProperties tableProperties) {
        schema = tableProperties.getSchema();
        compactionFilesBatchSize = tableProperties.getInt(COMPACTION_FILES_BATCH_SIZE);
        factory = new CompactionFactory(instanceProperties, tableProperties);
        leafStrategy.init(instanceProperties, tableProperties, factory);
        shouldCreateJobsStrategy.init(instanceProperties, tableProperties);
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
                long maxNumberOfJobsToCreate = shouldCreateJobsStrategy.maxCompactionJobsToCreate(
                        partition, activeFilesWithJobId, activeFilesWithNoJobId);
                if (maxNumberOfJobsToCreate < 1) {
                    continue;
                }
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

    private List<CompactionJob> createJobsForNonLeafPartition(
            Partition partition, List<FileInfo> fileInfos, Map<String, Partition> partitionIdToPartition) {

        return createBatches(partition, fileInfos, batchFiles -> {
            // Create job for these files
            LOGGER.info("Creating a job to compact {} files and split into 2 partitions (parent partition is {})",
                    batchFiles.size(), partition);
            List<String> childPartitions = partitionIdToPartition.get(partition.getId()).getChildPartitionIds();
            Partition leftPartition = partitionIdToPartition.get(childPartitions.get(0));
            Partition rightPartition = partitionIdToPartition.get(childPartitions.get(1));
            Object splitPoint = leftPartition.getRegion()
                    .getRange(schema.getRowKeyFieldNames().get(partition.getDimension()))
                    .getMax();
            LOGGER.info("Split point is {}", splitPoint);

            return factory.createSplittingCompactionJob(batchFiles,
                    partition.getId(),
                    leftPartition.getId(),
                    rightPartition.getId(),
                    splitPoint,
                    partition.getDimension());
        });
    }

    protected List<CompactionJob> createBatches(
            Partition partition, List<FileInfo> fileInfos, Function<List<FileInfo>, CompactionJob> buildJob) {

        List<CompactionJob> compactionJobs = new ArrayList<>();
        List<FileInfo> filesInAscendingOrder = getFilesInAscendingOrder(partition, fileInfos);

        // Iterate through files, creating jobs for batches of maximumNumberOfFilesToCompact files
        List<FileInfo> filesForJob = new ArrayList<>();
        for (FileInfo fileInfo : filesInAscendingOrder) {
            filesForJob.add(fileInfo);
            if (filesForJob.size() >= compactionFilesBatchSize) {
                compactionJobs.add(buildJob.apply(filesForJob));
                filesForJob.clear();
            }
        }

        // If there are any files left (even just 1), create a job for them
        if (!filesForJob.isEmpty()) {
            compactionJobs.add(buildJob.apply(filesForJob));
        }
        return compactionJobs;
    }

}
