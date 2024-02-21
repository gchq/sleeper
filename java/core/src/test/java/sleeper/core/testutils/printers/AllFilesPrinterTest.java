
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
