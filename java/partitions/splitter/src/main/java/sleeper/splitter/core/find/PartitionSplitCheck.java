/*
 * Copyright 2022-2025 Crown Copyright
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
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.FileReference;
import sleeper.core.table.TableStatus;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;

public class PartitionSplitCheck {
    public static final Logger LOGGER = LoggerFactory.getLogger(PartitionSplitCheck.class);

    private final TableStatus table;
    private final Partition partition;
    private final List<FileReference> partitionFileReferences;
    private final List<FileReference> filesWhollyInPartition;
    private final long estimatedRowsFromReferencesInPartitionTree;
    private final long estimatedRowsFromReferencesInPartition;
    private final long knownRowsWhollyInPartition;
    private final long splitThreshold;

    private PartitionSplitCheck(TableStatus table, Partition partition,
            List<FileReference> partitionFileReferences,
            List<FileReference> filesWhollyInPartition,
            long estimatedRowsFromReferencesInPartitionTree, long estimatedRowsFromReferencesInPartition,
            long knownRowsWhollyInPartition, long splitThreshold) {
        this.table = table;
        this.partition = partition;
        this.partitionFileReferences = partitionFileReferences;
        this.filesWhollyInPartition = filesWhollyInPartition;
        this.estimatedRowsFromReferencesInPartitionTree = estimatedRowsFromReferencesInPartitionTree;
        this.estimatedRowsFromReferencesInPartition = estimatedRowsFromReferencesInPartition;
        this.knownRowsWhollyInPartition = knownRowsWhollyInPartition;
        this.splitThreshold = splitThreshold;
    }

    public List<FileReference> getPartitionFileReferences() {
        return partitionFileReferences;
    }

    public long getEstimatedRowsFromReferencesInPartitionTree() {
        return estimatedRowsFromReferencesInPartitionTree;
    }

    public long getEstimatedRowsFromReferencesInPartition() {
        return estimatedRowsFromReferencesInPartition;
    }

    public long getKnownRowsWhollyInPartition() {
        return knownRowsWhollyInPartition;
    }

    public boolean isNeedsSplitting() {
        return knownRowsWhollyInPartition >= splitThreshold;
    }

    public boolean maySplitIfCompacted() {
        if (!partition.isLeafPartition()) {
            return false;
        }
        return estimatedRowsFromReferencesInPartitionTree >= splitThreshold;
    }

    Optional<FindPartitionToSplitResult> splitIfNecessary() {
        LOGGER.info("Analyzed partition {} of table {}", partition.getId(), table);
        LOGGER.info("Estimated rows from file references in partition tree: {}", estimatedRowsFromReferencesInPartitionTree);
        LOGGER.info("Estimated rows from file references in partition: {}", estimatedRowsFromReferencesInPartition);
        LOGGER.info("Known exact rows from files wholly in partition: {}", knownRowsWhollyInPartition);
        if (knownRowsWhollyInPartition >= splitThreshold) {
            LOGGER.info("Partition {} needs splitting (split threshold is {})", partition.getId(), splitThreshold);
            return Optional.of(new FindPartitionToSplitResult(table.getTableUniqueId(), partition, filesWhollyInPartition));
        } else {
            LOGGER.info("Partition {} does not need splitting (split threshold is {})", partition.getId(), splitThreshold);
            return Optional.empty();
        }
    }

    public static PartitionSplitCheck fromFilesInPartition(TableProperties properties, PartitionTree partitionTree, Partition partition, Map<String, List<FileReference>> fileReferencesByPartition) {
        List<FileReference> partitionFileReferences = fileReferencesByPartition.getOrDefault(partition.getId(), List.of());
        long estimatedInPartitionFromTree = estimateRowsInPartitionFromTree(partition, partitionTree, fileReferencesByPartition);
        long estimatedInPartition = partitionFileReferences.stream().mapToLong(FileReference::getNumberOfRows).sum();
        List<FileReference> filesWhollyInPartition = partitionFileReferences.stream().filter(FileReference::onlyContainsDataForThisPartition).collect(toUnmodifiableList());
        long known = filesWhollyInPartition.stream().filter(not(FileReference::isCountApproximate)).mapToLong(FileReference::getNumberOfRows).sum();
        return new PartitionSplitCheck(properties.getStatus(), partition,
                partitionFileReferences, filesWhollyInPartition,
                estimatedInPartitionFromTree, estimatedInPartition, known,
                properties.getLong(PARTITION_SPLIT_THRESHOLD));
    }

    private static long estimateRowsInPartitionFromTree(Partition partition, PartitionTree tree, Map<String, List<FileReference>> fileReferencesByPartition) {
        return estimateRowsInPartitionDescendents(partition, tree, fileReferencesByPartition)
                + estimateRowsInPartitionAndAncestors(partition, tree, fileReferencesByPartition);
    }

    private static long estimateRowsInPartitionDescendents(Partition partition, PartitionTree tree, Map<String, List<FileReference>> fileReferencesByPartition) {
        return tree.descendentsOf(partition)
                .flatMap(descendent -> fileReferencesByPartition.getOrDefault(descendent.getId(), List.of()).stream())
                .mapToLong(FileReference::getNumberOfRows)
                .sum();
    }

    private static long estimateRowsInPartitionAndAncestors(Partition partition, PartitionTree tree, Map<String, List<FileReference>> fileReferencesByPartition) {
        Partition current = partition;
        long count = 0;
        long treeDivisor = 1;
        do {
            List<FileReference> partitionFiles = fileReferencesByPartition.getOrDefault(current.getId(), List.of());
            long partitionCount = partitionFiles.stream().mapToLong(FileReference::getNumberOfRows).sum();
            count += partitionCount / treeDivisor;
            treeDivisor *= 2;
            current = tree.getParent(current);
        } while (current != null);
        return count;
    }

}
