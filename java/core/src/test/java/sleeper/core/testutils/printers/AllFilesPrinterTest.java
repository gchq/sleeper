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
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReferenceFactory;

import java.time.Instant;
import java.util.List;

import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.FilesReportTestHelper.activeAndReadyForGCFilesReport;

public class AllFilesPrinterTest {

    private final Schema schema = schemaWithKey("key", new StringType());
    private final PartitionsBuilder partitions = new PartitionsBuilder(schema);

    @Test
    void shouldPrintReferencedAndUnreferencedFiles() {
        // Given
        partitions.rootFirst("root")
                .splitToNewChildren("root", "L", "R", "row-50");

        // When
        FileReferenceFactory fileReferenceFactory = fileReferenceFactory();
        String printed = AllFilesPrinter.printFiles(partitions.buildTree(),
                activeAndReadyForGCFilesReport(Instant.now(),
                        List.of(fileReferenceFactory.partitionFile("L", 10)),
                        List.of("oldFile1.parquet", "oldFile2.parquet")));

        // Then see approved output
        Approvals.verify(printed);
    }

    private FileReferenceFactory fileReferenceFactory() {
        return FileReferenceFactory.from(partitions.buildTree());
    }
}
