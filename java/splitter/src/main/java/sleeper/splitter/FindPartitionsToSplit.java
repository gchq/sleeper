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
package sleeper.splitter;

import com.amazonaws.services.sqs.AmazonSQS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.core.partition.Partition;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;

/**
 * This finds partitions that need splitting. It does this by querying the
 * {@link StateStore} for {@link FileInfo}s for all active files. This information
 * is used to calculate the number of records in each partition. If a partition
 * needs splitting a {@link SplitPartitionJobCreator} is run. That will send the
 * definition of a splitting job to an SQS queue.
 */
public class FindPartitionsToSplit {
    private static final Logger LOGGER = LoggerFactory.getLogger(FindPartitionsToSplit.class);
    private final String tableName;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStore stateStore;
    private final long splitThreshold;
    private final int maxFilesInJob;
    private final AmazonSQS sqs;
    private final String sqsUrl;

    public FindPartitionsToSplit(
            String tableName,
            TablePropertiesProvider tablePropertiesProvider,
            StateStore stateStore,
            int maxFilesInJob,
            AmazonSQS sqs,
            String sqsUrl) {
        this.tableName = tableName;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStore = stateStore;
        this.splitThreshold = this.tablePropertiesProvider.getTableProperties(tableName).getLong(PARTITION_SPLIT_THRESHOLD);
        this.maxFilesInJob = maxFilesInJob;
        this.sqs = sqs;
        this.sqsUrl = sqsUrl;
    }

    public void run() throws StateStoreException, IOException {
        LOGGER.info("Running FindPartitionsToSplit for table {}, split threshold is {}", tableName, splitThreshold);
        
        List<FileInfo> activeFileInfos = stateStore.getActiveFiles();
        LOGGER.info("There are {} active files in table {}", activeFileInfos.size(), tableName);

        List<Partition> leafPartitions = stateStore.getLeafPartitions();
        LOGGER.info("There are {} leaf partitions in table {}", leafPartitions.size(), tableName);
        for (Partition partition : leafPartitions) {
            splitPartitionIfNecessary(partition, activeFileInfos);
        }
    }

    private void splitPartitionIfNecessary(Partition partition, List<FileInfo> fileInfos) throws StateStoreException, IOException {
        // Find files in this partition
        List<FileInfo> relevantFiles = getRelevantFileInfos(partition, fileInfos);
        // Calculate number of records in partition
        long numberOfRecordsInPartition = relevantFiles.stream().map(FileInfo::getNumberOfRecords).mapToLong(Long::longValue).sum();
        LOGGER.info("Number of records in partition {} of table {} is {}", partition.getId(), tableName, numberOfRecordsInPartition);
        if (numberOfRecordsInPartition >= splitThreshold) {
            LOGGER.info("Partition {} needs splitting as (split threshold is {})", partition.getId(), splitThreshold);
            // If there are more than PartitionSplittingMaxFilesInJob files then pick the largest ones.
            List<String> filesForJob = new ArrayList<>();
            if (relevantFiles.size() < maxFilesInJob) {
                filesForJob.addAll(relevantFiles.stream().map(FileInfo::getFilename).collect(Collectors.toList()));
            } else {
                // Compare method has f2 first to sort in reverse order
                filesForJob.addAll(
                        relevantFiles
                                .stream()
                                .sorted((f1, f2) -> Long.compare(f2.getNumberOfRecords(), f1.getNumberOfRecords()))
                                .limit(maxFilesInJob)
                                .map(FileInfo::getFilename)
                                .collect(Collectors.toList())
                );
            }
            // Create job and call run to send to job queue
            SplitPartitionJobCreator partitionJobCreator = new SplitPartitionJobCreator(tableName, tablePropertiesProvider, partition, filesForJob, sqsUrl, sqs);
            partitionJobCreator.run();
        } else {
            LOGGER.info("Partition {} does not need splitting (split threshold is {})", partition.getId(), splitThreshold);
        }
    }

    public static List<FileInfo> getRelevantFileInfos(Partition partition, List<FileInfo> fileInfos) {
        List<FileInfo> relevantFiles = new ArrayList<>();
        for (FileInfo fileInfo : fileInfos) {
            if (fileInfo.getPartitionId().equals(partition.getId())) {
                relevantFiles.add(fileInfo);
            }
        }
        return relevantFiles;
    }
}
