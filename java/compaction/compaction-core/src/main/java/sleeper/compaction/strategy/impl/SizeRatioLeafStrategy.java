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
import java.util.stream.Collectors;

import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.SIZE_RATIO_COMPACTION_STRATEGY_RATIO;

public class SizeRatioLeafStrategy implements LeafPartitionCompactionStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(SizeRatioLeafStrategy.class);

    private int ratio;
    private int compactionFilesBatchSize;
    private CompactionJobFactory factory;

    @Override
    public void init(InstanceProperties instanceProperties, TableProperties tableProperties) {
        ratio = tableProperties.getInt(SIZE_RATIO_COMPACTION_STRATEGY_RATIO);
        compactionFilesBatchSize = tableProperties.getInt(COMPACTION_FILES_BATCH_SIZE);
        this.factory = new CompactionJobFactory(instanceProperties, tableProperties);
    }

    @Override
    public List<CompactionJob> createJobsForLeafPartition(String partitionId, CompactionStrategyIndex index) {
        // Find files that meet criteria, i.e. sum of file sizes excluding largest
        // is >= ratio * largest file size.
        List<FileReference> filesThatMeetCriteria = getListOfFilesThatMeetsCriteria(index.getFilesWithNoJobIdInPartition(partitionId));
        if (null == filesThatMeetCriteria || filesThatMeetCriteria.isEmpty()) {
            LOGGER.info("For partition {} there is no list of files that meet the criteria", partitionId);
            return List.of();
        }
        LOGGER.debug("For partition {} there is a list of {} files that meet the criteria", partitionId, filesThatMeetCriteria.size());

        // Iterate through these files, batching into groups of compactionFilesBatchSize
        // and creating a job for each group as long as it meets the criteria.
        List<CompactionJob> compactionJobs = new ArrayList<>();
        if (filesThatMeetCriteria.size() <= compactionFilesBatchSize) {
            compactionJobs.add(factory.createCompactionJob(filesThatMeetCriteria, partitionId));
        } else {
            int position = 0;
            List<FileReference> files = new ArrayList<>(filesThatMeetCriteria);
            while (position < files.size()) {
                List<FileReference> filesForJob = new ArrayList<>();
                int j;
                for (j = 0; j < compactionFilesBatchSize && position + j < files.size(); j++) {
                    filesForJob.add(files.get(position + j));
                }
                // Create job for these files if they meet criteria
                List<Long> fileSizes = filesForJob.stream().map(FileReference::getNumberOfRecords).collect(Collectors.toList());
                if (meetsCriteria(fileSizes)) {
                    LOGGER.info("Creating a job to compact {} files in partition {}", filesForJob.size(), partitionId);
                    compactionJobs.add(factory.createCompactionJob(filesForJob, partitionId));
                    filesForJob.clear();
                    position += j;
                } else {
                    position++;
                }
            }
        }

        return compactionJobs;
    }

    private List<FileReference> getListOfFilesThatMeetsCriteria(List<FileReference> fileReferences) {
        List<FileReference> filesInAscendingOrder = new ArrayList<>(fileReferences);

        while (filesInAscendingOrder.size() > 1) {
            List<Long> fileSizes = filesInAscendingOrder.stream().map(FileReference::getNumberOfRecords).collect(Collectors.toList());
            if (meetsCriteria(fileSizes)) {
                return filesInAscendingOrder;
            } else {
                filesInAscendingOrder.remove(filesInAscendingOrder.size() - 1);
            }
        }
        return null;
    }

    private boolean meetsCriteria(List<Long> fileSizesInAscendingOrder) {
        if (fileSizesInAscendingOrder.isEmpty() || 1 == fileSizesInAscendingOrder.size()) {
            return false;
        }
        long largestFileSize = fileSizesInAscendingOrder.get(fileSizesInAscendingOrder.size() - 1);
        LOGGER.info("Largest file size is {}", largestFileSize);
        long sumOfOtherFileSizes = 0L;
        for (int i = 0; i < fileSizesInAscendingOrder.size() - 1; i++) {
            sumOfOtherFileSizes += fileSizesInAscendingOrder.get(i);
        }
        LOGGER.info("Sum of other file sizes is {}", sumOfOtherFileSizes);
        LOGGER.info("Ratio * largestFileSize <= sumOfOtherFileSizes {}", (ratio * largestFileSize <= sumOfOtherFileSizes));
        return ratio * largestFileSize <= sumOfOtherFileSizes;
    }
}
