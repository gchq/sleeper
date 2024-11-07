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
package sleeper.splitter.core.find;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.table.TableStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static sleeper.core.properties.instance.PartitionSplittingProperty.MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;

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

        PartitionTree partitionTree = new PartitionTree(stateStore.getAllPartitions());
        List<Partition> leafPartitions = partitionTree.getLeafPartitions();
        LOGGER.info("There are {} leaf partitions in table {}", leafPartitions.size(), table);

        Map<String, List<FileReference>> fileReferencesByPartition = fileReferences.stream()
                .collect(groupingBy(FileReference::getPartitionId));

        List<FindPartitionToSplitResult> results = new ArrayList<>();
        for (Partition partition : leafPartitions) {
            PartitionSplitCheck.fromFilesInPartition(tableProperties, partitionTree, partition, fileReferencesByPartition)
                    .splitIfNecessary().ifPresent(results::add);
        }
        return results;
    }

    @FunctionalInterface
    public interface JobSender {
        void send(SplitPartitionJobDefinition job);
    }
}
