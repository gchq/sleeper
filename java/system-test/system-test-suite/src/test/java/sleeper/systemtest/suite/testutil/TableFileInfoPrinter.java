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
import sleeper.core.partition.PartitionTree;
import sleeper.core.statestore.FileInfo;
import sleeper.core.table.TableIdentity;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static sleeper.systemtest.suite.testutil.TablesPrinter.printForAllTables;

public class TableFileInfoPrinter {

    private TableFileInfoPrinter() {
    }

    public static String printExpectedFilesForAllTables(
            List<TableIdentity> tables, PartitionTree partitions, List<FileInfo> activeFiles) {
        return printTableFilesExpectingIdentical(
                tables.stream().collect(Collectors.toMap(TableIdentity::getTableName, table -> partitions)),
                tables.stream().collect(Collectors.toMap(TableIdentity::getTableName, table -> activeFiles)));
    }

    public static String printTableFilesExpectingIdentical(
            Map<String, PartitionTree> partitionsByTable, Map<String, List<FileInfo>> activeFilesByTable) {
        return printForAllTables(activeFilesByTable.keySet(), table ->
                printFiles(partitionsByTable.get(table), activeFilesByTable.get(table)));
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
            String partitionName = TablePartitionsPrinter.buildPartitionName(partition, partitionTree);
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

}
