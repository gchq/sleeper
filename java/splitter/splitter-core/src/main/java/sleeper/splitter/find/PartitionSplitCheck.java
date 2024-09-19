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

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.statestore.FileReference;
import sleeper.core.table.TableStatus;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;

public class PartitionSplitCheck {
    public static final Logger LOGGER = LoggerFactory.getLogger(PartitionSplitCheck.class);

    private final TableStatus table;
    private final Partition partition;
    private final List<FileReference> partitionFileReferences;
    private final List<FileReference> filesWhollyInPartition;
    private final long estimatedRecordsFromReferencesInPartitionTree;
    private final long estimatedRecordsFromReferencesInPartition;
    private final long knownRecordsReferencedInPartition;
    private final long splitThreshold;

    private PartitionSplitCheck(TableStatus table, Partition partition,
            List<FileReference> partitionFileReferences,
            List<FileReference> filesWhollyInPartition,
            long estimatedRecordsFromReferencesInPartitionTree, long estimatedRecordsFromReferencesInPartition,
            long knownRecordsReferencedInPartition, long splitThreshold) {
        this.table = table;
        this.partition = partition;
        this.partitionFileReferences = partitionFileReferences;
        this.filesWhollyInPartition = filesWhollyInPartition;
        this.estimatedRecordsFromReferencesInPartitionTree = estimatedRecordsFromReferencesInPartitionTree;
        this.estimatedRecordsFromReferencesInPartition = estimatedRecordsFromReferencesInPartition;
        this.knownRecordsReferencedInPartition = knownRecordsReferencedInPartition;
        this.splitThreshold = splitThreshold;
    }

    public List<FileReference> getPartitionFileReferences() {
        return partitionFileReferences;
    }

    public long getEstimatedRecordsFromReferencesInPartitionTree() {
        return estimatedRecordsFromReferencesInPartitionTree;
    }

    public long getEstimatedRecordsFromReferencesInPartition() {
        return estimatedRecordsFromReferencesInPartition;
    }

    public long getKnownRecordsReferencedInPartition() {
        return knownRecordsReferencedInPartition;
    }

    public boolean isNeedsSplitting() {
        return knownRecordsReferencedInPartition >= splitThreshold;
    }

    public boolean maySplitIfCompacted() {
        return estimatedRecordsFromReferencesInPartitionTree >= splitThreshold;
    }

    public Optional<FindPartitionToSplitResult> splitIfNecessary() {
        LOGGER.info("Analyzed partition {} of table {}", partition.getId(), table);
        LOGGER.info("Estimated records from file references in partition tree: {}", estimatedRecordsFromReferencesInPartitionTree);
        LOGGER.info("Estimated records from file references in partition: {}", estimatedRecordsFromReferencesInPartition);
        LOGGER.info("Known exact count of records: {}", knownRecordsReferencedInPartition);
        LOGGER.info("Number of records in partition {} of table {} is {}", partition.getId(), table, knownRecordsReferencedInPartition);
        if (knownRecordsReferencedInPartition >= splitThreshold) {
            LOGGER.info("Partition {} needs splitting (split threshold is {})", partition.getId(), splitThreshold);
            return Optional.of(new FindPartitionToSplitResult(table.getTableUniqueId(), partition, filesWhollyInPartition));
        } else {
            LOGGER.info("Partition {} does not need splitting (split threshold is {})", partition.getId(), splitThreshold);
            return Optional.empty();
        }
    }

    public static PartitionSplitCheck fromFilesInPartition(TableProperties properties, PartitionTree partitionTree, Partition partition, Map<String, List<FileReference>> fileReferencesByPartition) {
        List<FileReference> partitionFileReferences = fileReferencesByPartition.getOrDefault(partition.getId(), List.of());
        long estimatedInPartitionOrAncestors = streamFilesInPartitionOrAncestors(partition, partitionTree, fileReferencesByPartition).mapToLong(FileReference::getNumberOfRecords).sum();
        long estimatedInPartition = partitionFileReferences.stream().mapToLong(FileReference::getNumberOfRecords).sum();
        long known = partitionFileReferences.stream().filter(not(FileReference::isCountApproximate)).mapToLong(FileReference::getNumberOfRecords).sum();
        List<FileReference> filesWhollyInPartition = partitionFileReferences.stream().filter(FileReference::onlyContainsDataForThisPartition).collect(toUnmodifiableList());
        return new PartitionSplitCheck(properties.getStatus(), partition,
                partitionFileReferences, filesWhollyInPartition,
                estimatedInPartitionOrAncestors, estimatedInPartition, known,
                properties.getLong(PARTITION_SPLIT_THRESHOLD));
    }

    private static Stream<FileReference> streamFilesInPartitionOrAncestors(
            Partition partition, PartitionTree tree, Map<String, List<FileReference>> fileReferencesByPartition) {
        return Stream.concat(Stream.of(partition), tree.ancestorsOf(partition))
                .map(Partition::getId)
                .flatMap(partitionId -> fileReferencesByPartition.getOrDefault(partitionId, List.of()).stream());
    }

}
