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
package sleeper.splitter.find;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.statestore.StateStoreProvider;
import sleeper.core.partition.Partition;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static sleeper.configuration.properties.instance.PartitionSplittingProperty.MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;

/**
 * This finds partitions that need splitting. It does this by querying the {@link StateStore} for {@link FileReference}s
 * for all active files. This information is used to calculate the number of records in each partition. If a partition
 * needs splitting a {@link JobSender} is run. That will send the definition of a splitting job to an SQS queue.
 */
public class FindPartitionsToSplit {
    private static final Logger LOGGER = LoggerFactory.getLogger(FindPartitionsToSplit.class);
    private final InstanceProperties instanceProperties;
    private final StateStoreProvider stateStoreProvider;
    private final JobSender jobSender;

    public FindPartitionsToSplit(
            InstanceProperties instanceProperties,
            StateStoreProvider stateStoreProvider,
            JobSender jobSender) {
        this.instanceProperties = instanceProperties;
        this.stateStoreProvider = stateStoreProvider;
        this.jobSender = jobSender;
    }

    public void run(TableProperties tableProperties) throws StateStoreException {
        findPartitionsToSplit(tableProperties, stateStoreProvider.getStateStore(tableProperties));
    }

    private void findPartitionsToSplit(TableProperties tableProperties, StateStore stateStore) throws StateStoreException {
        List<FindPartitionToSplitResult> results = getResults(tableProperties, stateStore);
        int maxFilesInJob = instanceProperties.getInt(MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB);
        for (FindPartitionToSplitResult result : results) {
            // If there are more than PartitionSplittingMaxFilesInJob files then pick the largest ones.
            List<String> filesForJob = new ArrayList<>();
            if (result.getRelevantFiles().size() < maxFilesInJob) {
                filesForJob.addAll(result.getRelevantFiles().stream().map(FileReference::getFilename).collect(Collectors.toList()));
            } else {
                // Compare method has f2 first to sort in reverse order
                filesForJob.addAll(
                        result.getRelevantFiles()
                                .stream()
                                .sorted((f1, f2) -> Long.compare(f2.getNumberOfRecords(), f1.getNumberOfRecords()))
                                .limit(maxFilesInJob)
                                .map(FileReference::getFilename)
                                .collect(Collectors.toList()));
            }
            // Create job and call run to send to job queue
            SplitPartitionJobDefinition job = new SplitPartitionJobDefinition(
                    tableProperties.getStatus().getTableUniqueId(), result.getPartition(), filesForJob);
            jobSender.send(job);
        }
    }

    public static List<FindPartitionToSplitResult> getResults(
            TableProperties tableProperties, StateStore stateStore) throws StateStoreException {
        TableStatus table = tableProperties.getStatus();
        long splitThreshold = tableProperties.getLong(PARTITION_SPLIT_THRESHOLD);
        LOGGER.info("Running FindPartitionsToSplit for table {}, split threshold is {}", table, splitThreshold);

        List<FileReference> fileReferences = stateStore.getFileReferences();
        LOGGER.info("There are {} file references in table {}", fileReferences.size(), table);

        List<Partition> leafPartitions = stateStore.getLeafPartitions();
        LOGGER.info("There are {} leaf partitions in table {}", leafPartitions.size(), table);

        Map<String, List<FileReference>> fileReferencesByPartition = fileReferences.stream()
                .collect(groupingBy(FileReference::getPartitionId));

        List<FindPartitionToSplitResult> results = new ArrayList<>();
        for (Partition partition : leafPartitions) {
            List<FileReference> partitionFiles = fileReferencesByPartition.getOrDefault(partition.getId(), List.of());
            splitPartitionIfNecessary(table, splitThreshold, partition, partitionFiles).ifPresent(results::add);
        }
        return results;
    }

    private static Optional<FindPartitionToSplitResult> splitPartitionIfNecessary(
            TableStatus table, long splitThreshold, Partition partition, List<FileReference> fileReferences) {
        PartitionSplitCheck check = PartitionSplitCheck.fromFilesInPartition(splitThreshold, fileReferences);
        LOGGER.info("Number of records in partition {} of table {} is {}", partition.getId(), table, check.getKnownRecordsInPartition());
        if (check.isNeedsSplitting()) {
            LOGGER.info("Partition {} needs splitting (split threshold is {})", partition.getId(), splitThreshold);
            return Optional.of(new FindPartitionToSplitResult(table.getTableUniqueId(), partition, check.getFilesToComputeSplitPointsFrom()));
        } else {
            LOGGER.info("Partition {} does not need splitting (split threshold is {})", partition.getId(), splitThreshold);
            return Optional.empty();
        }
    }

    public static List<FileReference> getFilesInPartition(Partition partition, List<FileReference> fileReferences) {
        return fileReferences.stream()
                .filter(file -> file.getPartitionId().equals(partition.getId()))
                .filter(FileReference::onlyContainsDataForThisPartition)
                .collect(Collectors.toUnmodifiableList());
    }

    @FunctionalInterface
    public interface JobSender {
        void send(SplitPartitionJobDefinition job);
    }
}
