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
package sleeper.compaction.core.strategy.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.core.strategy.CompactionStrategyIndex.FilesInPartition;
import sleeper.compaction.core.strategy.LeafPartitionCompactionStrategy;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.FileReference;

import java.util.ArrayList;
import java.util.List;

import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;

/**
 * A simple leaf partition compaction strategy to compact all files over the batch size. Iterates through a list of
 * file references for a partition, sorted in increasing order of the number of records they contain, creating
 * compaction jobs with at most compactionFilesBatchSize files in each.
 */
public class BasicLeafStrategy implements LeafPartitionCompactionStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(BasicLeafStrategy.class);

    private CompactionJobFactory factory;
    private int compactionFilesBatchSize;

    @Override
    public void init(InstanceProperties instanceProperties, TableProperties tableProperties, CompactionJobFactory factory) {
        compactionFilesBatchSize = tableProperties.getInt(COMPACTION_FILES_BATCH_SIZE);
        this.factory = factory;
    }

    @Override
    public List<CompactionJob> createJobsForLeafPartition(FilesInPartition filesInPartition) {
        List<CompactionJob> compactionJobs = new ArrayList<>();
        List<FileReference> filesWithNoJobId = filesInPartition.getFilesWithNoJobIdInAscendingOrder();

        // Iterate through files, creating jobs for batches of compactionFilesBatchSize files
        List<FileReference> filesForJob = new ArrayList<>();
        for (FileReference fileReference : filesWithNoJobId) {
            filesForJob.add(fileReference);
            if (filesForJob.size() >= compactionFilesBatchSize) {
                // Create job for these files
                LOGGER.info("Creating a job to compact {} files in partition {} in table {}",
                        filesForJob.size(), filesInPartition.getPartitionId(), filesInPartition.getTableStatus());
                compactionJobs.add(factory.createCompactionJob(filesForJob, filesInPartition.getPartitionId()));
                filesForJob.clear();
            }
        }
        if (compactionJobs.isEmpty()) {
            LOGGER.info("Not enough unassigned files in partition {} in table {} to create a batch of size {}",
                    filesInPartition.getPartitionId(), filesInPartition.getTableStatus(),
                    compactionFilesBatchSize);
        }
        return compactionJobs;
    }
}
