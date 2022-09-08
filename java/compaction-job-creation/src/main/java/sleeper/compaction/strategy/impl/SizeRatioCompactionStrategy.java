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
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.partition.Partition;
import sleeper.statestore.FileInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.table.TableProperty.SIZE_RATIO_COMPACTION_STRATEGY_RATIO;

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

    private int ratio;
    private int maxConcurrentCompactionJobsPerPartition;

    public SizeRatioCompactionStrategy() {
        super();
    }

    @Override
    public void init(InstanceProperties instanceProperties, TableProperties tableProperties) {
        super.init(instanceProperties, tableProperties);
        ratio = tableProperties.getInt(SIZE_RATIO_COMPACTION_STRATEGY_RATIO);
        maxConcurrentCompactionJobsPerPartition = tableProperties.getInt(TableProperty.SIZE_RATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION);
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
                long numConcurrentCompactionJobs = getNumberOfCurrentCompactionJobs(partitionId, activeFilesWithJobId);
                if (numConcurrentCompactionJobs >= maxConcurrentCompactionJobsPerPartition) {
                    LOGGER.info("Not creating compaction jobs for partition {} as there are already {} running compaction jobs", partitionId, numConcurrentCompactionJobs);
                    continue;
                }
                long maxNumberOfJobsToCreate = maxConcurrentCompactionJobsPerPartition - numConcurrentCompactionJobs;
                LOGGER.info("Max jobs to create = {}", maxNumberOfJobsToCreate);
                List<CompactionJob> jobs = createJobsForLeafPartition(partition, activeFilesWithNoJobId);
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

    private long getNumberOfCurrentCompactionJobs(String partitionId, List<FileInfo> activeFilesWithJobId) {
        return activeFilesWithJobId.stream()
                .filter(f -> f.getPartitionId().equals(partitionId))
                .map(FileInfo::getJobId)
                .collect(Collectors.toSet())
                .size();
    }

    @Override
    protected List<CompactionJob> createJobsForLeafPartition(Partition partition, List<FileInfo> fileInfos) {
        // Find files that meet criteria, i.e. sum of file sizes excluding largest
        // is >= ratio * largest file size.
        List<FileInfo> filesThatMeetCriteria = getListOfFilesThatMeetsCriteria(partition, fileInfos);
        if (null == filesThatMeetCriteria || filesThatMeetCriteria.isEmpty()) {
            LOGGER.info("For partition {} there is no list of files that meet the criteria", partition.getId());
            return Collections.EMPTY_LIST;
        }
        LOGGER.info("For partition {} there is a list of {} files that meet the criteria", partition.getId(), filesThatMeetCriteria.size());

        // Iterate through these files, batching into groups of compactionFilesBatchSize
        // and creating a job for each group as long as it meets the criteria.
        List<CompactionJob> compactionJobs = new ArrayList<>();
        if (filesThatMeetCriteria.size() <= compactionFilesBatchSize) {
            compactionJobs.add(factory.createCompactionJob(filesThatMeetCriteria, partition.getId(), tableBucket));
        } else {
            int position = 0;
            List<FileInfo> files = new ArrayList<>(filesThatMeetCriteria);
            while (position < files.size()) {
                List<FileInfo> filesForJob = new ArrayList<>();
                int j;
                for (j = 0; j < compactionFilesBatchSize && position + j < files.size(); j++) {
                    filesForJob.add(files.get(position + j));
                }
                // Create job for these files if they meet criteria
                List<Long> fileSizes = filesForJob.stream().map(FileInfo::getNumberOfRecords).collect(Collectors.toList());
                if (meetsCriteria(fileSizes)) {
                    LOGGER.info("Creating a job to compact {} files in partition {}",
                            filesForJob.size(), partition.getId());
                    compactionJobs.add(factory.createCompactionJob(filesForJob, partition.getId(), tableBucket));
                    filesForJob.clear();
                    position += j;
                } else {
                    position++;
                }
            }
        }

        return compactionJobs;
    }

    private List<FileInfo> getListOfFilesThatMeetsCriteria(Partition partition, List<FileInfo> fileInfos) {
        List<FileInfo> filesInAscendingOrder = getFilesInAscendingOrder(partition, fileInfos);

        while (filesInAscendingOrder.size() > 1) {
            List<Long> fileSizes = filesInAscendingOrder.stream().map(FileInfo::getNumberOfRecords).collect(Collectors.toList());
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
