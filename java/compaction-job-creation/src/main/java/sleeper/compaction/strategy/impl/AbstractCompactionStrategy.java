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
import java.util.List;
import java.util.Map;

import static sleeper.compaction.strategy.impl.CompactionUtils.getFilesInAscendingOrder;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;

public abstract class AbstractCompactionStrategy implements CompactionStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCompactionStrategy.class);

    protected Schema schema;
    protected int compactionFilesBatchSize;
    protected CompactionFactory factory;
    protected final LeafPartitionCompactionStrategy leafStrategy;

    protected AbstractCompactionStrategy(LeafPartitionCompactionStrategy leafStrategy) {
        this.leafStrategy = leafStrategy;
    }

    @Override
    public void init(InstanceProperties instanceProperties, TableProperties tableProperties) {
        factory = new CompactionFactory(instanceProperties, tableProperties);
        schema = tableProperties.getSchema();
        compactionFilesBatchSize = tableProperties.getInt(COMPACTION_FILES_BATCH_SIZE);
    }

    protected List<CompactionJob> createJobsForNonLeafPartition(Partition partition,
                                                                List<FileInfo> fileInfos,
                                                                Map<String, Partition> partitionIdToPartition) {
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
                List<String> childPartitions = partitionIdToPartition.get(partition.getId()).getChildPartitionIds();
                Partition leftPartition = partitionIdToPartition.get(childPartitions.get(0));
                Partition rightPartition = partitionIdToPartition.get(childPartitions.get(1));
                Object splitPoint = leftPartition.getRegion()
                        .getRange(schema.getRowKeyFieldNames().get(partition.getDimension()))
                        .getMax();
                LOGGER.info("Split point is {}", splitPoint);

                compactionJobs.add(factory.createSplittingCompactionJob(filesForJob,
                        partition.getId(),
                        leftPartition.getId(),
                        rightPartition.getId(),
                        splitPoint,
                        partition.getDimension()));
                filesForJob.clear();
            }
        }

        // If there are any files left (even just 1), create a job for them
        if (!filesForJob.isEmpty()) {
            // Create job for these files
            LOGGER.info("Creating a job to compact {} files in partition {}",
                    filesForJob.size(), partition);
            List<String> childPartitions = partitionIdToPartition.get(partition.getId()).getChildPartitionIds();
            Partition leftPartition = partitionIdToPartition.get(childPartitions.get(0));
            Partition rightPartition = partitionIdToPartition.get(childPartitions.get(1));
            Object splitPoint = leftPartition.getRegion()
                    .getRange(schema.getRowKeyFieldNames().get(partition.getDimension()))
                    .getMax();
            LOGGER.info("Split point is {}", splitPoint);

            compactionJobs.add(factory.createSplittingCompactionJob(filesForJob,
                    partition.getId(),
                    leftPartition.getId(),
                    rightPartition.getId(),
                    splitPoint,
                    partition.getDimension()));
            filesForJob.clear();
        }
        return compactionJobs;
    }

}
