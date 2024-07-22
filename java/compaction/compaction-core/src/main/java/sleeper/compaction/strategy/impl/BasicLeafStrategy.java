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
package sleeper.compaction.strategy.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.compaction.strategy.CompactionStrategyIndex;
import sleeper.compaction.strategy.LeafPartitionCompactionStrategy;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.statestore.FileReference;

import java.util.ArrayList;
import java.util.List;

import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

/**
 * A simple leaf partition compaction strategy to compact all files over the batch size. Iterates through a list of
 * file references for a partition, sorted in increasing order of the number of records they contain, creating
 * compaction jobs with at most compactionFilesBatchSize files in each.
 */
public class BasicLeafStrategy implements LeafPartitionCompactionStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(BasicLeafStrategy.class);

    private CompactionJobFactory factory;
    private String tableName;
    private int compactionFilesBatchSize;

    @Override
    public void init(InstanceProperties instanceProperties, TableProperties tableProperties) {
        tableName = tableProperties.get(TABLE_NAME);
        compactionFilesBatchSize = tableProperties.getInt(COMPACTION_FILES_BATCH_SIZE);
        this.factory = new CompactionJobFactory(instanceProperties, tableProperties);
    }

    @Override
    public List<CompactionJob> createJobsForLeafPartition(String partitionId, CompactionStrategyIndex index) {
        List<CompactionJob> compactionJobs = new ArrayList<>();
        List<FileReference> filesWithNoJobId = index.getFilesInPartition(partitionId).getFilesWithNoJobIdInAscendingOrder();

        // Iterate through files, creating jobs for batches of compactionFilesBatchSize files
        List<FileReference> filesForJob = new ArrayList<>();
        for (FileReference fileReference : filesWithNoJobId) {
            filesForJob.add(fileReference);
            if (filesForJob.size() >= compactionFilesBatchSize) {
                // Create job for these files
                LOGGER.info("Creating a job to compact {} files in partition {} in table {}",
                        filesForJob.size(), partitionId, tableName);
                compactionJobs.add(factory.createCompactionJob(filesForJob, partitionId));
                filesForJob.clear();
            }
        }
        if (filesWithNoJobId.isEmpty()) {
            LOGGER.info("No unassigned files in partition {} in table {}, cannot create jobs", partitionId, tableName);
        } else if (compactionJobs.isEmpty()) {
            LOGGER.info("Not enough unassigned files in partition {} in table {} to create a batch of size {}", partitionId, tableName, compactionFilesBatchSize);
        }
        return compactionJobs;
    }
}
