/*
 * Copyright 2022-2026 Crown Copyright
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

package sleeper.core.testutils.printers;

import org.assertj.core.presentation.Representation;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.RegionSerDe;
import sleeper.core.schema.Schema;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

/**
 * Prints partition trees as text, for readable assertions in tests. Intended to be used in approval tests, for visual
 * comparison against previously generated values. Uses {@link TablesPrinter} to consolidate the generated output for
 * multiple tables.
 */
public class PartitionsPrinter {

    private PartitionsPrinter() {
    }

    /**
     * Generates a string with information about partitions for all provided tables. The tables must have the same
     * schema in order to deserialise the partition regions correctly.
     *
     * @param  schema            the schema for all tables
     * @param  partitionsByTable the map of table name to partition tree
     * @return                   a string describing the partitions
     */
    public static String printTablePartitionsExpectingIdentical(Schema schema, Map<String, PartitionTree> partitionsByTable) {
        return TablesPrinter.printForAllTablesExcludingNames(partitionsByTable.keySet(), table -> printPartitions(schema, partitionsByTable.get(table)));
    }

    /**
     * Generates a string with information about partitions.
     *
     * @param  schema        the schema for all tables
     * @param  partitionTree the partition tree
     * @return               a string describing the partitions
     */
    public static String printPartitions(Schema schema, PartitionTree partitionTree) {
        ToStringPrintWriter printer = new ToStringPrintWriter();
        PrintStream out = printer.getPrintStream();
        RegionSerDe regionSerDe = new RegionSerDe(schema);
        partitionTree.traverseLeavesFirst().forEach(partition -> {
            String locationName = partitionTree.buildLocationName(partition);
            if (partition.isLeafPartition()) {
                out.println("Leaf partition at " + locationName + ":");
            } else {
                out.println("Partition at " + locationName + ":");
            }
            out.println(regionSerDe.toJson(partition.getRegion()));
        });
        return printer.toString();
    }

    /**
     * Returns an AssertJ representation that can describe a PartitionTree or a list of partitions.
     *
     * @param  tableProperties the Sleeper table properties
     * @return                 the representation
     */
    public static Representation representation(TableProperties tableProperties) {
        return obj -> printPartitionsAndIds(tableProperties.getSchema(), asPartitionTree(obj));
    }

    /**
     * Generates a string with information about partitions, and the IDs of those partitions. The IDs are printed in
     * order of the partitions' locations in the tree, starting with the leaf partitions.
     *
     * @param  schema        the schema of the Sleeper table
     * @param  partitionTree the partition tree
     * @return               a string describing the partitions and their IDs
     */
    public static String printPartitionsAndIds(Schema schema, PartitionTree partitionTree) {
        return printPartitions(schema, partitionTree)
                + "\n\nPartition IDs: " + partitionTree.traverseLeavesFirst().map(Partition::getId).toList();
    }

    private static PartitionTree asPartitionTree(Object object) {
        if (object instanceof PartitionTree tree) {
            return tree;
        } else if (object instanceof List<?> list) {
            return new PartitionTree((List<Partition>) list);
        } else {
            throw new IllegalArgumentException("Could not read partitions as object is not a PartitionTree or List");
        }
    }
}
