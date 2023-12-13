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
import sleeper.core.statestore.FileInfo;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableFileInfoPrinter {

    public static String printExpectedForAllTables(
            Map<String, PartitionTree> partitionsByTable, List<FileInfo> activeFiles) {
        return printTableFilesExpectingIdentical(partitionsByTable,
                partitionsByTable.keySet().stream()
                        .collect(Collectors.toMap(table -> table, table -> activeFiles)));
    }

    public static String printTableFilesExpectingIdentical(
            Map<String, PartitionTree> partitionsByTable, Map<String, List<FileInfo>> activeFilesByTable) {
        List<String> tableNames = activeFilesByTable.keySet().stream().collect(Collectors.toUnmodifiableList());
        Map<String, List<String>> tableNamesByPrintedValue = tableNames.stream()
                .collect(Collectors.groupingBy(table ->
                        printFiles(partitionsByTable.get(table), activeFilesByTable.get(table))));
        List<Map.Entry<String, List<String>>> printedSortedByFrequency = tableNamesByPrintedValue.entrySet().stream()
                .sorted(Comparator.comparing(entry -> entry.getValue().size()))
                .collect(Collectors.toUnmodifiableList());
        ToStringPrintStream printer = new ToStringPrintStream();
        PrintStream out = printer.getPrintStream();

        for (Map.Entry<String, List<String>> entry : printedSortedByFrequency) {
            String printed = entry.getKey();
            List<String> printedForTables = entry.getValue();
            int frequency = printedForTables.size();
            if (frequency == 1) {
                if (printedSortedByFrequency.size() == 1) {
                    out.println("One table named " + printedForTables.get(0));
                } else {
                    out.println("Different for table named " + printedForTables.get(0));
                }
            } else {
                out.println("Same for " + frequency + " tables");
            }
            out.println(printed);
        }
        return printer.toString();
    }

    public static String printFiles(PartitionTree partitionTree, List<FileInfo> files) {
        ToStringPrintStream printer = new ToStringPrintStream();
        PrintWriter out = printer.getPrintWriter();
        out.println("Active files:");
        Map<String, List<FileInfo>> filesByPartition = files.stream()
                .collect(Collectors.groupingBy(FileInfo::getPartitionId));
        partitionTree.traverseLeavesFirst().forEach(partition -> {
            List<FileInfo> partitionFiles = filesByPartition.get(partition.getId());
            if (partitionFiles == null) {
                return;
            }
            String partitionName = buildPartitionName(partition, partitionTree);
            out.print("Partition " + partitionName + ":");
            if (partitionFiles.size() > 1) {
                out.println();
            } else {
                out.print(" ");
            }
            for (FileInfo file : partitionFiles) {
                out.println(file.getNumberOfRecords() + " records in file " + file.getFilename());
            }
        });
        out.flush();
        return printer.toString();
    }

    private static String buildPartitionName(Partition partition, PartitionTree tree) {
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
