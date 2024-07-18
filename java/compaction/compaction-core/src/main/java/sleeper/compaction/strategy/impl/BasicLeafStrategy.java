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
import sleeper.compaction.strategy.LeafPartitionCompactionStrategy;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.statestore.FileReference;

import java.util.ArrayList;
import java.util.List;

import static sleeper.compaction.strategy.impl.CompactionUtils.getFilesInAscendingOrder;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

/**
 * A simple leaf partition compaction strategy to compact all files over the batch size. Lists the active files for a
 * partition in increasing order of the number of records they contain, and iterates through this list creating
 * compaction jobs with at most compactionFilesBatchSize files in each.
 */
public class BasicLeafStrategy implements LeafPartitionCompactionStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(BasicLeafStrategy.class);

    private CompactionJobFactory factory;
    private String tableName;
    private int compactionFilesBatchSize;

    @Override
    public void init(InstanceProperties instanceProperties, TableProperties tableProperties, CompactionJobFactory factory) {
        tableName = tableProperties.get(TABLE_NAME);
        compactionFilesBatchSize = tableProperties.getInt(COMPACTION_FILES_BATCH_SIZE);
        this.factory = factory;
    }

    @Override
    public List<CompactionJob> createJobsForLeafPartition(String partitionId, List<FileReference> fileReferences) {
        List<CompactionJob> compactionJobs = new ArrayList<>();
        List<FileReference> filesInAscendingOrder = getFilesInAscendingOrder(tableName, fileReferences);

        // Iterate through files, creating jobs for batches of compactionFilesBatchSize files
        List<FileReference> filesForJob = new ArrayList<>();
        for (FileReference fileReference : filesInAscendingOrder) {
            filesForJob.add(fileReference);
            if (filesForJob.size() >= compactionFilesBatchSize) {
                // Create job for these files
                LOGGER.info("Creating a job to compact {} files in partition {} in table {}",
                        filesForJob.size(), partitionId, tableName);
                compactionJobs.add(factory.createCompactionJob(filesForJob, partitionId));
                filesForJob.clear();
            }
        }
        return compactionJobs;
    }
}
