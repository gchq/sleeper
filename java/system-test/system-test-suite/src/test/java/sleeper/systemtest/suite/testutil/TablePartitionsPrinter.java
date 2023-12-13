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

package sleeper.systemtest.suite.testutil;

import sleeper.configuration.properties.format.ToStringPrintStream;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.range.RegionSerDe;
import sleeper.core.schema.Schema;

import java.io.PrintStream;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import static sleeper.systemtest.suite.testutil.TablesPrinter.printForAllTables;

public class TablePartitionsPrinter {

    private TablePartitionsPrinter() {
    }

    public static String printExpectedForAllTables(Schema schema, Collection<String> tableNames, PartitionTree tree) {
        return printTablePartitionsExpectingIdentical(schema, tableNames.stream()
                .collect(Collectors.toMap(table -> table, table -> tree)));
    }

    public static String printTablePartitionsExpectingIdentical(Schema schema, Map<String, PartitionTree> partitionsByTable) {
        return printForAllTables(partitionsByTable.keySet(), table ->
                printPartitions(schema, partitionsByTable.get(table)));
    }

    public static String printPartitions(Schema schema, PartitionTree partitionTree) {
        ToStringPrintStream printer = new ToStringPrintStream();
        PrintStream out = printer.getPrintStream();
        RegionSerDe regionSerDe = new RegionSerDe(schema);
        partitionTree.traverseLeavesFirst().forEach(partition -> {
            String partitionName = buildPartitionName(partition, partitionTree);
            out.println("Partition at " + partitionName);
            out.println(regionSerDe.toJson(partition.getRegion()));
        });
        return printer.toString();
    }

    public static String buildPartitionName(Partition partition, PartitionTree tree) {
        String parentId = partition.getParentPartitionId();
        if (parentId == null) {
            return "root";
        }
        String partitionId = partition.getId();
        StringBuilder name = new StringBuilder();
        while (parentId != null) {
            Partition parent = tree.getPartition(parentId);
            name.append(getPartitionLabel(partitionId, parent));
            partitionId = parent.getId();
            parentId = parent.getParentPartitionId();
        }
        return name.reverse().toString();
    }

    private static char getPartitionLabel(String partitionId, Partition parent) {
        int index = parent.getChildPartitionIds().indexOf(partitionId);
        if (index == 0) {
            return 'L';
        } else if (index == 1) {
            return 'R';
        } else {
            throw new IllegalStateException("Unexpected index " + index + " for partition " + partitionId + " in parent: " + parent);
        }
    }
}
