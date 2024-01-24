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
package sleeper.clients.status.report.filestatus;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.clients.status.report.FilesStatusReport;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.SplitFileReference;
import sleeper.core.statestore.StateStore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithNoPartitions;

public class FilesStatusReportTest {
    private final Schema schema = Schema.builder().rowKeyFields(new Field("key1", new StringType())).build();
    private final Instant lastStateStoreUpdate = Instant.parse("2022-08-22T14:20:00.001Z");
    private final StateStore stateStore = inMemoryStateStoreWithNoPartitions();

    @BeforeEach
    void setUp() {
        stateStore.fixTime(lastStateStoreUpdate);
    }

    @Test
    public void shouldReportFilesStatusGivenOneActiveFilePerLeafPartition() throws Exception {
        // Given
        PartitionTree partitions = new PartitionsBuilder(schema)
                .rootFirst("0")
                .splitToNewChildren("0", "1", "H", "ggg")
                .splitToNewChildren("1", "2", "G", "fff")
                .splitToNewChildren("2", "3", "F", "eee")
                .splitToNewChildren("3", "4", "E", "ddd")
                .splitToNewChildren("4", "5", "D", "ccc")
                .splitToNewChildren("5", "6", "C", "bbb")
                .splitToNewChildren("6", "A", "B", "aaa")
                .buildTree();
        stateStore.initialise(partitions.getAllPartitions());
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(partitions);
        stateStore.addFiles(List.of(
                fileReferenceFactory.partitionFile("A", 50000001),
                fileReferenceFactory.partitionFile("B", 50000002),
                fileReferenceFactory.partitionFile("C", 50000003),
                fileReferenceFactory.partitionFile("D", 50000004),
                fileReferenceFactory.partitionFile("E", 50000005),
                fileReferenceFactory.partitionFile("F", 50000006),
                fileReferenceFactory.partitionFile("G", 50000007),
                fileReferenceFactory.partitionFile("H", 50000008)));

        // When / Then
        assertThat(verboseReportString(StandardFileStatusReporter::new))
                .isEqualTo(example("reports/filestatus/standard/oneActiveFilePerLeaf.txt"));
        assertThatJson(verboseReportString(JsonFileStatusReporter::new))
                .isEqualTo(example("reports/filestatus/json/oneActiveFilePerLeaf.json"));
    }

    @Test
    public void shouldReportFilesStatusGivenActiveFileInLeafAndMiddlePartition() throws Exception {
        // Given
        PartitionTree partitions = new PartitionsBuilder(schema)
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "mmm")
                .splitToNewChildren("B", "D", "E", "ggg")
                .buildTree();
        stateStore.initialise(partitions.getAllPartitions());
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(partitions);
        stateStore.addFiles(List.of(
                fileReferenceFactory.partitionFile("D", 50000001),
                fileReferenceFactory.partitionFile("B", 50000002)));

        // When / Then
        assertThat(verboseReportString(StandardFileStatusReporter::new))
                .isEqualTo(example("reports/filestatus/standard/leafAndMiddleFile.txt"));
        assertThatJson(verboseReportString(JsonFileStatusReporter::new))
                .isEqualTo(example("reports/filestatus/json/leafAndMiddleFile.json"));
    }

    @Test
    public void shouldReportFilesStatusGivenFilesWithNoReferencesBelowMaxCount() throws Exception {
        // Given
        PartitionTree partitions = new PartitionsBuilder(schema)
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "mmm")
                .buildTree();
        stateStore.initialise(partitions.getAllPartitions());
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(partitions);
        List<FileReference> files = List.of(
                fileReferenceFactory.partitionFile("B", "file1.parquet", 100),
                fileReferenceFactory.partitionFile("B", "file2.parquet", 100));
        stateStore.addFiles(files);
        stateStore.atomicallyUpdateJobStatusOfFiles("job1", files);
        stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(
                "job1", "B", List.of("file1.parquet", "file2.parquet"),
                List.of(fileReferenceFactory.partitionFile("B", "file3.parquet", 200)));

        // When / Then
        assertThat(verboseReportString(StandardFileStatusReporter::new))
                .isEqualTo(example("reports/filestatus/standard/filesWithNoReferencesBelowMaxCount.txt"));
        assertThatJson(verboseReportString(JsonFileStatusReporter::new))
                .isEqualTo(example("reports/filestatus/json/filesWithNoReferencesBelowMaxCount.json"));
    }

    @Test
    public void shouldReportFilesStatusGivenFilesWithNoReferencesAboveMaxCount() throws Exception {
        // Given
        PartitionTree partitions = new PartitionsBuilder(schema)
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "mmm")
                .buildTree();
        stateStore.initialise(partitions.getAllPartitions());
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(partitions);
        List<FileReference> files = List.of(
                fileReferenceFactory.partitionFile("B", "file1.parquet", 100),
                fileReferenceFactory.partitionFile("B", "file2.parquet", 100),
                fileReferenceFactory.partitionFile("B", "file3.parquet", 100),
                fileReferenceFactory.partitionFile("B", "file4.parquet", 100));
        stateStore.addFiles(files);
        stateStore.atomicallyUpdateJobStatusOfFiles("job1", files);
        stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("job1", "B",
                List.of("file1.parquet", "file2.parquet", "file3.parquet", "file4.parquet"),
                List.of(fileReferenceFactory.partitionFile("B", "file5.parquet", 400)));
        int maxFilesWithNoReferences = 3;

        // When / Then
        assertThat(verboseReportStringWithMaxFilesWithNoReferences(StandardFileStatusReporter::new, maxFilesWithNoReferences))
                .isEqualTo(example("reports/filestatus/standard/filesWithNoReferencesAboveMaxCount.txt"));
        assertThatJson(verboseReportStringWithMaxFilesWithNoReferences(JsonFileStatusReporter::new, maxFilesWithNoReferences))
                .isEqualTo(example("reports/filestatus/json/filesWithNoReferencesAboveMaxCount.json"));
    }

    @Test
    public void shouldReportFilesStatusWhenSomeFilesHaveBeenSplit() throws Exception {
        // Given
        PartitionTree partitions = new PartitionsBuilder(schema)
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "mmm")
                .buildTree();
        stateStore.initialise(partitions.getAllPartitions());
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(partitions, lastStateStoreUpdate);
        FileReference rootFile = fileReferenceFactory.partitionFile("A", "not-split.parquet", 1000);
        FileReference pendingSplit = fileReferenceFactory.partitionFile("B", "pending-split.parquet", 2000);
        FileReference oldFile = fileReferenceFactory.partitionFile("A", "split.parquet", 2000L);
        FileReference newFile1 = SplitFileReference.referenceForChildPartition(oldFile, "B")
                .toBuilder().lastStateStoreUpdateTime(lastStateStoreUpdate).build();
        FileReference newFile2 = SplitFileReference.referenceForChildPartition(oldFile, "C")
                .toBuilder().lastStateStoreUpdateTime(lastStateStoreUpdate).build();
        stateStore.addFiles(List.of(rootFile, pendingSplit, oldFile));
        stateStore.atomicallyUpdateJobStatusOfFiles("job1", List.of(oldFile));
        stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(
                "job1", "A", List.of("split.parquet"), List.of(newFile1, newFile2));

        // When / Then
        assertThat(verboseReportString(StandardFileStatusReporter::new))
                .isEqualTo(example("reports/filestatus/standard/splitFile.txt"));
        assertThatJson(verboseReportString(JsonFileStatusReporter::new))
                .isEqualTo(example("reports/filestatus/json/splitFile.json"));
    }

    private static String example(String path) throws IOException {
        URL url = FilesStatusReportTest.class.getClassLoader().getResource(path);
        return IOUtils.toString(Objects.requireNonNull(url), Charset.defaultCharset());
    }

    private String verboseReportString(Function<PrintStream, FileStatusReporter> getReporter) throws Exception {
        return verboseReportStringWithMaxFilesWithNoReferences(getReporter, 100);
    }

    private String verboseReportStringWithMaxFilesWithNoReferences(Function<PrintStream, FileStatusReporter> getReporter, int maxFilesWithNoReferences) throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        FileStatusReporter reporter = getReporter.apply(
                new PrintStream(os, false, StandardCharsets.UTF_8.displayName()));
        new FilesStatusReport(stateStore, maxFilesWithNoReferences, true, reporter).run();
        return os.toString(StandardCharsets.UTF_8);
    }

}
