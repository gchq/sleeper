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
package sleeper.splitter.core.extend;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.transactionlog.transaction.impl.ExtendPartitionTreeTransaction;
import sleeper.sketches.Sketches;
import sleeper.splitter.core.split.FindPartitionSplitPoint;
import sleeper.splitter.core.split.SketchesForSplitting;
import sleeper.splitter.core.split.SplitPartitionResult;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;

/**
 * Creates a transaction to extend the partition tree based on sketches of data in the existing leaf partitions.
 */
public class ExtendPartitionTreeBasedOnSketches {

    private final Schema schema;
    private final int minLeafPartitions;
    private final Supplier<String> idSupplier;

    private ExtendPartitionTreeBasedOnSketches(Schema schema, int minLeafPartitions, Supplier<String> idSupplier) {
        this.schema = schema;
        this.minLeafPartitions = minLeafPartitions;
        this.idSupplier = idSupplier;
    }

    public static ExtendPartitionTreeBasedOnSketches forBulkImport(TableProperties tableProperties) {
        return forBulkImport(tableProperties, () -> UUID.randomUUID().toString());
    }

    public static ExtendPartitionTreeBasedOnSketches forBulkImport(TableProperties tableProperties, Supplier<String> idSupplier) {
        return new ExtendPartitionTreeBasedOnSketches(
                tableProperties.getSchema(),
                tableProperties.getInt(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT),
                idSupplier);
    }

    public ExtendPartitionTreeTransaction createTransaction(PartitionTree tree, Map<String, Sketches> partitionIdToSketches) {
        List<Partition> originalLeafPartitions = tree.getLeafPartitions();
        SplitsTracker tracker = new SplitsTracker();
        PartitionSketchIndex sketchIndex = PartitionSketchIndex.from(schema, partitionIdToSketches);
        List<Partition> workingPartitions = originalLeafPartitions;
        while (tracker.getNumLeafPartitions() < minLeafPartitions) {
            workingPartitions = splitLeaves(sketchIndex, workingPartitions)
                    .peek(tracker::recordSplit)
                    .peek(sketchIndex::recordSplit)
                    .flatMap(SplitPartitionResult::streamChildPartitions)
                    .toList();
        }
        return tracker.buildTransaction();
    }

    private Stream<SplitPartitionResult> splitLeaves(PartitionSketchIndex sketchIndex, List<Partition> leafPartitions) {
        return leafPartitions.stream()
                .flatMap(partition -> {
                    SketchesForSplitting sketches = sketchIndex.get(partition.getId());
                    return FindPartitionSplitPoint.getResultIfSplittable(schema, partition, sketches, idSupplier).stream();
                });
    }

}
