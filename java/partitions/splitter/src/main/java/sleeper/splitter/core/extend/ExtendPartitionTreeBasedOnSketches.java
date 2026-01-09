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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Creates a transaction to extend the partition tree based on sketches of data in the existing leaf partitions.
 */
public class ExtendPartitionTreeBasedOnSketches {

    private final Schema schema;
    private final Supplier<String> idSupplier;

    private ExtendPartitionTreeBasedOnSketches(Schema schema, Supplier<String> idSupplier) {
        this.schema = schema;
        this.idSupplier = idSupplier;
    }

    public static ExtendPartitionTreeBasedOnSketches forBulkImport(TableProperties tableProperties) {
        return forBulkImport(tableProperties, () -> UUID.randomUUID().toString());
    }

    public static ExtendPartitionTreeBasedOnSketches forBulkImport(TableProperties tableProperties, Supplier<String> idSupplier) {
        return new ExtendPartitionTreeBasedOnSketches(
                tableProperties.getSchema(),
                idSupplier);
    }

    public ExtendPartitionTreeTransaction createTransaction(PartitionTree tree, Map<String, Sketches> partitionIdToSketches) {
        List<Partition> leafPartitions = tree.getLeafPartitions();
        List<Partition> updatedPartitions = new ArrayList<>();
        List<Partition> newPartitions = new ArrayList<>();
        for (Partition partition : leafPartitions) {
            Sketches sketches = partitionIdToSketches.get(partition.getId());
            FindPartitionSplitPoint.getResultIfSplittable(schema, partition, List.of(sketches), idSupplier)
                    .ifPresent(result -> {
                        updatedPartitions.add(result.getParentPartition());
                        newPartitions.add(result.getLeftChild());
                        newPartitions.add(result.getRightChild());
                    });
        }
        return new ExtendPartitionTreeTransaction(updatedPartitions, newPartitions);
    }

}
