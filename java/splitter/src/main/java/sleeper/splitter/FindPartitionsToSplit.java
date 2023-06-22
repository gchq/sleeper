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
package sleeper.splitter;

import com.amazonaws.services.sqs.AmazonSQS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.partition.Partition;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

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
    private final TableProperties tableProperties;
    private final StateStore stateStore;
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
        this.tableProperties = tablePropertiesProvider.getTableProperties(tableName);
        this.stateStore = stateStore;
        this.maxFilesInJob = maxFilesInJob;
        this.sqs = sqs;
        this.sqsUrl = sqsUrl;
    }

    public void run() throws StateStoreException, IOException {
        List<FindPartitionToSplitResult> results = getResults(tableProperties, stateStore);
        for (FindPartitionToSplitResult result : results) {
            // If there are more than PartitionSplittingMaxFilesInJob files then pick the largest ones.
            List<String> filesForJob = new ArrayList<>();
            if (result.getRelevantFiles().size() < maxFilesInJob) {
                filesForJob.addAll(result.getRelevantFiles().stream().map(FileInfo::getFilename).collect(Collectors.toList()));
            } else {
                // Compare method has f2 first to sort in reverse order
                filesForJob.addAll(
                        result.getRelevantFiles()
                                .stream()
                                .sorted((f1, f2) -> Long.compare(f2.getNumberOfRecords(), f1.getNumberOfRecords()))
                                .limit(maxFilesInJob)
                                .map(FileInfo::getFilename)
                                .collect(Collectors.toList())
                );
            }
            // Create job and call run to send to job queue
            SplitPartitionJobCreator partitionJobCreator = new SplitPartitionJobCreator(
                    tableName, tablePropertiesProvider, result.getPartition(), filesForJob, sqsUrl, sqs);
            partitionJobCreator.run();
        }
    }

    public static List<FindPartitionToSplitResult> getResults(
            TableProperties tableProperties, StateStore stateStore) throws StateStoreException {
        String tableName = tableProperties.get(TABLE_NAME);
        long splitThreshold = tableProperties.getLong(PARTITION_SPLIT_THRESHOLD);
        LOGGER.info("Running FindPartitionsToSplit for table {}, split threshold is {}", tableName, splitThreshold);

        List<FileInfo> activeFileInfos = stateStore.getActiveFiles();
        LOGGER.info("There are {} active files in table {}", activeFileInfos.size(), tableName);

        List<Partition> leafPartitions = stateStore.getLeafPartitions();
        LOGGER.info("There are {} leaf partitions in table {}", leafPartitions.size(), tableName);

        List<FindPartitionToSplitResult> results = new ArrayList<>();
        for (Partition partition : leafPartitions) {
            splitPartitionIfNecessary(tableName, splitThreshold, partition, activeFileInfos).ifPresent(results::add);
        }
        return results;
    }

    private static Optional<FindPartitionToSplitResult> splitPartitionIfNecessary(
            String tableName, long splitThreshold, Partition partition, List<FileInfo> activeFileInfos) {
        List<FileInfo> relevantFiles = getFilesInPartition(partition, activeFileInfos);
        PartitionSplitCheck check = PartitionSplitCheck.fromFilesInPartition(splitThreshold, relevantFiles);
        LOGGER.info("Number of records in partition {} of table {} is {}", partition.getId(), tableName, check.getNumberOfRecordsInPartition());
        if (check.isNeedsSplitting()) {
            LOGGER.info("Partition {} needs splitting (split threshold is {})", partition.getId(), splitThreshold);
            return Optional.of(new FindPartitionToSplitResult(partition, relevantFiles));
        } else {
            LOGGER.info("Partition {} does not need splitting (split threshold is {})", partition.getId(), splitThreshold);
            return Optional.empty();
        }
    }

    public static List<FileInfo> getFilesInPartition(Partition partition, List<FileInfo> activeFileInfos) {
        List<FileInfo> relevantFiles = new ArrayList<>();
        for (FileInfo fileInfo : activeFileInfos) {
            if (fileInfo.getPartitionId().equals(partition.getId())) {
                relevantFiles.add(fileInfo);
            }
        }
        return relevantFiles;
    }
}
