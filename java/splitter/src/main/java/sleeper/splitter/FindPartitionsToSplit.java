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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.partition.Partition;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableIdentity;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Map.entry;
import static sleeper.configuration.properties.instance.PartitionSplittingProperty.MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

/**
 * This finds partitions that need splitting. It does this by querying the
 * {@link StateStore} for {@link FileInfo}s for all active files. This information
 * is used to calculate the number of records in each partition. If a partition
 * needs splitting a {@link SQSSplitPartitionJobSender} is run. That will send the
 * definition of a splitting job to an SQS queue.
 */
public class FindPartitionsToSplit {
    private static final Logger LOGGER = LoggerFactory.getLogger(FindPartitionsToSplit.class);
    private final TableIdentity tableId;
    private final TableProperties tableProperties;
    private final StateStore stateStore;
    private final SplitPartitionJobSender jobSender;
    private final int maxFilesInJob;

    public FindPartitionsToSplit(
            InstanceProperties instanceProperties,
            TableProperties tableProperties,
            TablePropertiesProvider tablePropertiesProvider,
            StateStore stateStore,
            AmazonSQS sqs) {
        this(instanceProperties, tableProperties, stateStore,
                new SQSSplitPartitionJobSender(tablePropertiesProvider, instanceProperties, sqs));
    }

    public FindPartitionsToSplit(
            InstanceProperties instanceProperties,
            TableProperties tableProperties,
            StateStore stateStore,
            SplitPartitionJobSender jobSender) {
        this.tableId = tableProperties.getId();
        this.tableProperties = tableProperties;
        this.stateStore = stateStore;
        this.jobSender = jobSender;
        this.maxFilesInJob = instanceProperties.getInt(MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB);
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
            SplitPartitionJobDefinition job = new SplitPartitionJobDefinition(
                    tableId.getTableUniqueId(), result.getPartition(), filesForJob);
            jobSender.send(job);
        }
    }

    public static List<FindPartitionToSplitResult> getResults(
            TablePropertiesProvider propertiesProvider, StateStoreProvider stateStoreProvider) {

        // Collect all table properties and state stores first to avoid concurrency problems with providers
        List<TableProperties> tableProperties = propertiesProvider.streamAllTables()
                .collect(Collectors.toUnmodifiableList());
        Map<String, StateStore> stateStoreByTableId = tableProperties.stream()
                .map(properties -> entry(properties.get(TABLE_ID), stateStoreProvider.getStateStore(properties)))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

        return tableProperties.stream().parallel()
                .flatMap(properties -> {
                    try {
                        return getResults(properties, stateStoreByTableId.get(properties.get(TABLE_ID))).stream();
                    } catch (StateStoreException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toUnmodifiableList());
    }

    public static List<FindPartitionToSplitResult> getResults(
            TableProperties tableProperties, StateStore stateStore) throws StateStoreException {
        TableIdentity tableId = tableProperties.getId();
        long splitThreshold = tableProperties.getLong(PARTITION_SPLIT_THRESHOLD);
        LOGGER.info("Running FindPartitionsToSplit for table {}, split threshold is {}", tableId, splitThreshold);

        List<FileInfo> activeFileInfos = stateStore.getActiveFiles();
        LOGGER.info("There are {} active files in table {}", activeFileInfos.size(), tableId);

        List<Partition> leafPartitions = stateStore.getLeafPartitions();
        LOGGER.info("There are {} leaf partitions in table {}", leafPartitions.size(), tableId);

        List<FindPartitionToSplitResult> results = new ArrayList<>();
        for (Partition partition : leafPartitions) {
            splitPartitionIfNecessary(tableId, splitThreshold, partition, activeFileInfos).ifPresent(results::add);
        }
        return results;
    }

    private static Optional<FindPartitionToSplitResult> splitPartitionIfNecessary(
            TableIdentity tableId, long splitThreshold, Partition partition, List<FileInfo> activeFileInfos) {
        List<FileInfo> relevantFiles = getFilesInPartition(partition, activeFileInfos);
        PartitionSplitCheck check = PartitionSplitCheck.fromFilesInPartition(splitThreshold, relevantFiles);
        LOGGER.info("Number of records in partition {} of table {} is {}", partition.getId(), tableId, check.getNumberOfRecordsInPartition());
        if (check.isNeedsSplitting()) {
            LOGGER.info("Partition {} needs splitting (split threshold is {})", partition.getId(), splitThreshold);
            return Optional.of(new FindPartitionToSplitResult(tableId.getTableUniqueId(), partition, relevantFiles));
        } else {
            LOGGER.info("Partition {} does not need splitting (split threshold is {})", partition.getId(), splitThreshold);
            return Optional.empty();
        }
    }

    public static List<FileInfo> getFilesInPartition(Partition partition, List<FileInfo> activeFileInfos) {
        return activeFileInfos.stream()
                .filter(file -> file.getPartitionId().equals(partition.getId()))
                .filter(FileInfo::onlyContainsDataForThisPartition)
                .collect(Collectors.toUnmodifiableList());
    }
}
