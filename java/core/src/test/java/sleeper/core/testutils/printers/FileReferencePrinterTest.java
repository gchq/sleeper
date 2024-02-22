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

import org.approvaltests.Approvals;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.table.TableIdentity;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.FilesReportTestHelper.activeAndReadyForGCFiles;
import static sleeper.core.statestore.FilesReportTestHelper.activeFiles;
import static sleeper.core.statestore.FilesReportTestHelper.noFiles;
import static sleeper.core.statestore.SplitFileReference.referenceForChildPartition;

public class FileReferencePrinterTest {

    private final Schema schema = schemaWithKey("key", new StringType());
    private final PartitionsBuilder partitions = new PartitionsBuilder(schema);

    @Nested
    @DisplayName("Active files")
    class ActiveFiles {

        @Test
        void shouldPrintMultipleFilesInPartition() {
            // Given
            partitions.rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "row-50");

            // When
            FileReferenceFactory fileReferenceFactory = fileReferenceFactory();
            String printed = FileReferencePrinter.printFiles(partitions.buildTree(), List.of(
                    fileReferenceFactory.partitionFile("L", 10),
                    fileReferenceFactory.partitionFile("L", 20),
                    fileReferenceFactory.partitionFile("R", 30),
                    fileReferenceFactory.partitionFile("R", 40)));

            // Then see approved output
            Approvals.verify(printed);
        }

        @Test
        void shouldPrintPartialFiles() {
            // Given
            partitions.rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "row-50");
            FileReference file1 = fileReferenceFactory().rootFile("a.parquet", 100);
            FileReference file2 = fileReferenceFactory().rootFile("a.parquet", 200);

            // When
            String printed = FileReferencePrinter.printFiles(partitions.buildTree(), List.of(
                    referenceForChildPartition(file1, "L"),
                    referenceForChildPartition(file2, "L"),
                    referenceForChildPartition(file1, "R"),
                    referenceForChildPartition(file2, "R")));

            // Then see approved output
            Approvals.verify(printed);
        }

        @Test
        void shouldPrintFilesOnLeaves() {
            // Given
            partitions.rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "row-50")
                    .splitToNewChildren("L", "LL", "LR", "row-25")
                    .splitToNewChildren("R", "RL", "RR", "row-75")
                    .splitToNewChildren("LL", "LLL", "LLR", "row-12")
                    .splitToNewChildren("LR", "LRL", "LRR", "row-37")
                    .splitToNewChildren("RL", "RLL", "RLR", "row-62")
                    .splitToNewChildren("RR", "RRL", "RRR", "row-87");

            // When
            FileReferenceFactory fileReferenceFactory = fileReferenceFactory();
            String printed = FileReferencePrinter.printFiles(partitions.buildTree(), List.of(
                    fileReferenceFactory.partitionFile("LLL", 12),
                    fileReferenceFactory.partitionFile("LLR", 13),
                    fileReferenceFactory.partitionFile("LRL", 12),
                    fileReferenceFactory.partitionFile("LRR", 13),
                    fileReferenceFactory.partitionFile("RLL", 12),
                    fileReferenceFactory.partitionFile("RLR", 13),
                    fileReferenceFactory.partitionFile("RRL", 12),
                    fileReferenceFactory.partitionFile("RRR", 13)));

            // Then see approved output
            Approvals.verify(printed);
        }

        @Test
        void shouldOrderFilesByPartitionLocationInTree() {
            // Given
            partitions.rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "row-50")
                    .splitToNewChildren("L", "LL", "LR", "row-25")
                    .splitToNewChildren("R", "RL", "RR", "row-75")
                    .splitToNewChildren("LL", "LLL", "LLR", "row-12")
                    .splitToNewChildren("LR", "LRL", "LRR", "row-37")
                    .splitToNewChildren("RL", "RLL", "RLR", "row-62")
                    .splitToNewChildren("RR", "RRL", "RRR", "row-87");

            // When
            FileReferenceFactory fileReferenceFactory = fileReferenceFactory();
            String printed = FileReferencePrinter.printFiles(partitions.buildTree(), List.of(
                    fileReferenceFactory.partitionFile("L", 50),
                    fileReferenceFactory.partitionFile("LRL", 12),
                    fileReferenceFactory.partitionFile("root", 100),
                    fileReferenceFactory.partitionFile("RLL", 12),
                    fileReferenceFactory.partitionFile("RR", 12),
                    fileReferenceFactory.partitionFile("LLL", 13),
                    fileReferenceFactory.partitionFile("R", 12),
                    fileReferenceFactory.partitionFile("RRL", 13),
                    fileReferenceFactory.partitionFile("LLR", 25),
                    fileReferenceFactory.partitionFile("RLR", 50),
                    fileReferenceFactory.partitionFile("RRR", 100)));

            // Then see approved output
            Approvals.verify(printed);
        }

        @Test
        void shouldRenamePartitionsByLocation() {
            // Given
            partitions.rootFirst("base")
                    .splitToNewChildren("base", "l", "r", "row-50")
                    .splitToNewChildren("l", "ll", "lr", "row-25")
                    .splitToNewChildren("r", "rl", "rr", "row-75")
                    .splitToNewChildren("ll", "1", "2", "row-12")
                    .splitToNewChildren("lr", "3", "4", "row-37")
                    .splitToNewChildren("rl", "5", "6", "row-62")
                    .splitToNewChildren("rr", "7", "8", "row-87");

            // When
            FileReferenceFactory fileReferenceFactory = fileReferenceFactory();
            String printed = FileReferencePrinter.printFiles(partitions.buildTree(), List.of(
                    fileReferenceFactory.partitionFile("1", 12),
                    fileReferenceFactory.partitionFile("2", 13),
                    fileReferenceFactory.partitionFile("3", 12),
                    fileReferenceFactory.partitionFile("4", 13),
                    fileReferenceFactory.partitionFile("5", 12),
                    fileReferenceFactory.partitionFile("6", 13),
                    fileReferenceFactory.partitionFile("7", 12),
                    fileReferenceFactory.partitionFile("8", 13),
                    fileReferenceFactory.partitionFile("ll", 25),
                    fileReferenceFactory.partitionFile("l", 50),
                    fileReferenceFactory.partitionFile("base", 100)));

            // Then see approved output
            Approvals.verify(printed);
        }
    }

    @Nested
    @DisplayName("Multiple tables")
    class MultipleTables {

        @Test
        void shouldPrintFilesOnceWhenTwoTablesAreIdentical() {
            // Given
            partitions.rootFirst("root");
            List<FileReference> files = List.of(fileReferenceFactory().partitionFile("root", 10));

            // When
            String printed = FileReferencePrinter.printTableFilesExpectingIdentical(
                    Map.of("table-1", partitions.buildTree(), "table-2", partitions.buildTree()),
                    Map.of("table-1", files, "table-2", files));

            // Then see approved output
            Approvals.verify(printed);
        }

        @Test
        void shouldPrintDifferentFilesForOneTable() {
            // Given
            partitions.rootFirst("root");
            List<FileReference> files1 = List.of(fileReferenceFactory().partitionFile("root", 10));
            List<FileReference> files2 = List.of(fileReferenceFactory().partitionFile("root", 20));

            // When
            String printed = FileReferencePrinter.printTableFilesExpectingIdentical(
                    Map.of("table-1", partitions.buildTree(), "table-2", partitions.buildTree(), "table-3", partitions.buildTree()),
                    Map.of("table-1", files1, "table-2", files2, "table-3", files1));

            // Then see approved output
            Approvals.verify(printed);
        }

        @Test
        void shouldPrintOnlyOneTable() {
            // Given
            partitions.rootFirst("root");
            List<FileReference> files = List.of(fileReferenceFactory().partitionFile("root", 10));

            // When
            String printed = FileReferencePrinter.printTableFilesExpectingIdentical(
                    Map.of("table-1", partitions.buildTree()),
                    Map.of("table-1", files));

            // Then see approved output
            Approvals.verify(printed);
        }

        @Test
        void shouldPrintExpectedForTables() {
            // Given
            partitions.rootFirst("root");
            List<FileReference> files = List.of(fileReferenceFactory().partitionFile("root", 10));

            // When
            String printed = FileReferencePrinter.printExpectedFilesForAllTables(
                    List.of(table("table-1"), table("table-2")), partitions.buildTree(), files);

            // Then
            assertThat(printed).isEqualTo(FileReferencePrinter.printTableFilesExpectingIdentical(
                    Map.of("table-1", partitions.buildTree(), "table-2", partitions.buildTree()),
                    Map.of("table-1", files, "table-2", files)));
        }
    }

    @Nested
    @DisplayName("All files including unreferenced")
    class AllFiles {

        @Test
        void shouldPrintReferencedAndUnreferencedFiles() {
            // Given
            partitions.rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "row-50");

            // When
            FileReferenceFactory fileReferenceFactory = fileReferenceFactory();
            String printed = FileReferencePrinter.printFiles(partitions.buildTree(),
                    activeAndReadyForGCFiles(
                            List.of(fileReferenceFactory.partitionFile("L", 10)),
                            List.of("oldFile1.parquet", "oldFile2.parquet")));

            // Then see approved output
            Approvals.verify(printed);
        }

        @Test
        void shouldPrintFileSplitAcrossPartitions() {
            // Given
            partitions.rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "row-50");

            // When
            FileReference rootFile = fileReferenceFactory().rootFile(20);
            String printed = FileReferencePrinter.printFiles(partitions.buildTree(),
                    activeFiles(
                            referenceForChildPartition(rootFile, "L"),
                            referenceForChildPartition(rootFile, "R")));

            // Then see approved output
            Approvals.verify(printed);
        }

        @Test
        void shouldPrintNoFiles() {
            // Given
            partitions.rootFirst("root");

            // When
            String printed = FileReferencePrinter.printFiles(partitions.buildTree(), noFiles());

            // Then see approved output
            Approvals.verify(printed);
        }
    }

    private TableIdentity table(String name) {
        return TableIdentity.uniqueIdAndName(name, name);
    }

    private FileReferenceFactory fileReferenceFactory() {
        return FileReferenceFactory.from(partitions.buildTree());
    }
}
