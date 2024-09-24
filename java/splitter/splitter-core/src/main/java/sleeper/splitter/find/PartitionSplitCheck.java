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
    private final long knownRecordsWhollyInPartition;
    private final long splitThreshold;

    private PartitionSplitCheck(TableStatus table, Partition partition,
            List<FileReference> partitionFileReferences,
            List<FileReference> filesWhollyInPartition,
            long estimatedRecordsFromReferencesInPartitionTree, long estimatedRecordsFromReferencesInPartition,
            long knownRecordsWhollyInPartition, long splitThreshold) {
        this.table = table;
        this.partition = partition;
        this.partitionFileReferences = partitionFileReferences;
        this.filesWhollyInPartition = filesWhollyInPartition;
        this.estimatedRecordsFromReferencesInPartitionTree = estimatedRecordsFromReferencesInPartitionTree;
        this.estimatedRecordsFromReferencesInPartition = estimatedRecordsFromReferencesInPartition;
        this.knownRecordsWhollyInPartition = knownRecordsWhollyInPartition;
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

    public long getKnownRecordsWhollyInPartition() {
        return knownRecordsWhollyInPartition;
    }

    public boolean isNeedsSplitting() {
        return knownRecordsWhollyInPartition >= splitThreshold;
    }

    public boolean maySplitIfCompacted() {
        if (!partition.isLeafPartition()) {
            return false;
        }
        return estimatedRecordsFromReferencesInPartitionTree >= splitThreshold;
    }

    Optional<FindPartitionToSplitResult> splitIfNecessary() {
        LOGGER.info("Analyzed partition {} of table {}", partition.getId(), table);
        LOGGER.info("Estimated records from file references in partition tree: {}", estimatedRecordsFromReferencesInPartitionTree);
        LOGGER.info("Estimated records from file references in partition: {}", estimatedRecordsFromReferencesInPartition);
        LOGGER.info("Known exact records from files wholly in partition: {}", knownRecordsWhollyInPartition);
        if (knownRecordsWhollyInPartition >= splitThreshold) {
            LOGGER.info("Partition {} needs splitting (split threshold is {})", partition.getId(), splitThreshold);
            return Optional.of(new FindPartitionToSplitResult(table.getTableUniqueId(), partition, filesWhollyInPartition));
        } else {
            LOGGER.info("Partition {} does not need splitting (split threshold is {})", partition.getId(), splitThreshold);
            return Optional.empty();
        }
    }

    public static PartitionSplitCheck fromFilesInPartition(TableProperties properties, PartitionTree partitionTree, Partition partition, Map<String, List<FileReference>> fileReferencesByPartition) {
        List<FileReference> partitionFileReferences = fileReferencesByPartition.getOrDefault(partition.getId(), List.of());
        long estimatedInPartitionFromTree = estimateRecordsInPartitionFromTree(partition, partitionTree, fileReferencesByPartition);
        long estimatedInPartition = partitionFileReferences.stream().mapToLong(FileReference::getNumberOfRecords).sum();
        List<FileReference> filesWhollyInPartition = partitionFileReferences.stream().filter(FileReference::onlyContainsDataForThisPartition).collect(toUnmodifiableList());
        long known = filesWhollyInPartition.stream().filter(not(FileReference::isCountApproximate)).mapToLong(FileReference::getNumberOfRecords).sum();
        return new PartitionSplitCheck(properties.getStatus(), partition,
                partitionFileReferences, filesWhollyInPartition,
                estimatedInPartitionFromTree, estimatedInPartition, known,
                properties.getLong(PARTITION_SPLIT_THRESHOLD));
    }

    private static long estimateRecordsInPartitionFromTree(Partition partition, PartitionTree tree, Map<String, List<FileReference>> fileReferencesByPartition) {
        return estimateRecordsInPartitionDescendents(partition, tree, fileReferencesByPartition)
                + estimateRecordsInPartitionAndAncestors(partition, tree, fileReferencesByPartition);
    }

    private static long estimateRecordsInPartitionDescendents(Partition partition, PartitionTree tree, Map<String, List<FileReference>> fileReferencesByPartition) {
        return tree.descendentsOf(partition)
                .flatMap(descendent -> fileReferencesByPartition.getOrDefault(descendent.getId(), List.of()).stream())
                .mapToLong(FileReference::getNumberOfRecords)
                .sum();
    }

    private static long estimateRecordsInPartitionAndAncestors(Partition partition, PartitionTree tree, Map<String, List<FileReference>> fileReferencesByPartition) {
        Partition current = partition;
        long count = 0;
        long treeDivisor = 1;
        do {
            List<FileReference> partitionFiles = fileReferencesByPartition.getOrDefault(current.getId(), List.of());
            long partitionCount = partitionFiles.stream().mapToLong(FileReference::getNumberOfRecords).sum();
            count += partitionCount / treeDivisor;
            treeDivisor *= 2;
            current = tree.getParent(current);
        } while (current != null);
        return count;
    }

}
