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

package sleeper.core.testutils.printers;

import sleeper.core.partition.PartitionTree;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.FileReference;
import sleeper.core.table.TableStatus;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toUnmodifiableList;

public class FileReferencePrinter {

    private FileReferencePrinter() {
    }

    public static String printExpectedFilesForAllTables(
            List<TableStatus> tables, PartitionTree partitions, AllReferencesToAllFiles files) {
        return printTableFilesExpectingIdentical(
                tables.stream().collect(Collectors.toMap(TableStatus::getTableName, table -> partitions)),
                tables.stream().collect(Collectors.toMap(TableStatus::getTableName, table -> files)));
    }

    public static String printTableFilesExpectingIdentical(
            Map<String, PartitionTree> partitionsByTable, Map<String, AllReferencesToAllFiles> filesByTable) {
        return TablesPrinter.printForAllTables(filesByTable.keySet(),
                table -> printFiles(partitionsByTable.get(table), filesByTable.get(table)));
    }

    public static String printFiles(PartitionTree tree, AllReferencesToAllFiles files) {
        ToStringPrintStream printer = new ToStringPrintStream();
        PrintWriter out = printer.getPrintWriter();
        out.println("Unreferenced files: " + files.getFilesWithNoReferences().size());
        out.println("Referenced files: " + files.getFilesWithReferences().size());
        printFiles(tree, files, out);
        out.flush();
        return printer.toString();
    }

    private static void printFiles(PartitionTree partitionTree, AllReferencesToAllFiles files, PrintWriter out) {
        List<FileReference> references = sortFileReferences(files);
        out.println("File references: " + references.size());
        Map<String, List<FileReference>> filesByPartition = references.stream()
                .collect(Collectors.groupingBy(FileReference::getPartitionId));
        AtomicInteger partialCount = new AtomicInteger();
        Map<String, Integer> numberByPartialFilename = new HashMap<>();
        partitionTree.traverseLeavesFirst().forEach(partition -> {
            List<FileReference> partitionFiles = filesByPartition.get(partition.getId());
            if (partitionFiles == null) {
                return;
            }
            String locationName = PartitionsPrinter.buildPartitionLocationName(partition, partitionTree);
            out.print("Partition at " + locationName + ":");
            if (partitionFiles.size() > 1) {
                out.println();
            } else {
                out.print(" ");
            }
            for (FileReference file : partitionFiles) {
                out.print(file.getNumberOfRecords() + " records ");
                if (file.isCountApproximate()) {
                    out.print("(approx) ");
                }
                if (file.onlyContainsDataForThisPartition()) {
                    out.println("in file");
                } else {
                    int partialFileNumber = numberByPartialFilename.computeIfAbsent(
                            file.getFilename(), name -> partialCount.incrementAndGet());
                    out.println("in partial file " + partialFileNumber);
                }
            }
        });
    }

    private static List<FileReference> sortFileReferences(AllReferencesToAllFiles files) {
        return files.getFilesWithReferences().stream()
                .flatMap(file -> file.getInternalReferences().stream())
                .sorted(comparing(FileReference::getNumberOfRecords).reversed())
                .collect(toUnmodifiableList());
    }
}
