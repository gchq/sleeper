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

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;

import java.util.List;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.ReplaceFileReferencesRequest.replaceJobFileReferences;
import static sleeper.core.statestore.SplitFileReferenceRequest.splitFileToChildPartitions;

public class FilesStatusReportTest extends FilesStatusReportTestBase {

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
        stateStore.addFiles(List.of(
                fileReferenceFactory.partitionFile("B", "file1.parquet", 100),
                fileReferenceFactory.partitionFile("B", "file2.parquet", 100)));
        stateStore.assignJobIds(List.of(
                assignJobOnPartitionToFiles("job1", "B", List.of("file1.parquet", "file2.parquet"))));
        stateStore.atomicallyReplaceFileReferencesWithNewOnes(List.of(replaceJobFileReferences(
                "job1", List.of("file1.parquet", "file2.parquet"), fileReferenceFactory.partitionFile("B", "file3.parquet", 200))));

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
        stateStore.addFiles(List.of(
                fileReferenceFactory.partitionFile("B", "file1.parquet", 100),
                fileReferenceFactory.partitionFile("B", "file2.parquet", 100),
                fileReferenceFactory.partitionFile("B", "file3.parquet", 100),
                fileReferenceFactory.partitionFile("B", "file4.parquet", 100)));
        stateStore.assignJobIds(List.of(
                assignJobOnPartitionToFiles("job1", "B",
                        List.of("file1.parquet", "file2.parquet", "file3.parquet", "file4.parquet"))));
        stateStore.atomicallyReplaceFileReferencesWithNewOnes(List.of(replaceJobFileReferences(
                "job1", List.of("file1.parquet", "file2.parquet", "file3.parquet", "file4.parquet"),
                fileReferenceFactory.partitionFile("B", "file5.parquet", 400))));
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
        stateStore.addFiles(List.of(rootFile, pendingSplit, oldFile));
        stateStore.splitFileReferences(List.of(splitFileToChildPartitions(oldFile, "B", "C")));

        // When / Then
        assertThat(verboseReportString(StandardFileStatusReporter::new))
                .isEqualTo(example("reports/filestatus/standard/splitFile.txt"));
        assertThatJson(verboseReportString(JsonFileStatusReporter::new))
                .isEqualTo(example("reports/filestatus/json/splitFile.json"));
    }

    @Test
    public void shouldReportFilesStatusWhenPartitionsNoLongerExist() throws Exception {
        // Given
        PartitionTree partitions = new PartitionsBuilder(schema).rootFirst("A").buildTree();
        stateStore.initialise(partitions.getAllPartitions());
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(partitions, lastStateStoreUpdate);
        FileReference file1 = fileReferenceFactory.rootFile("file1.parquet", 1000L);
        FileReference file2 = fileReferenceFactory.rootFile("file2.parquet", 2000L);
        stateStore.initialise(new PartitionsBuilder(schema).rootFirst("B").buildList());
        stateStore.addFiles(List.of(file1, file2));

        // When / Then
        assertThat(verboseReportString(StandardFileStatusReporter::new))
                .isEqualTo(example("reports/filestatus/standard/fileWithPartitionThatNoLongerExists.txt"));
        assertThatJson(verboseReportString(JsonFileStatusReporter::new))
                .isEqualTo(example("reports/filestatus/json/fileWithPartitionThatNoLongerExists.json"));
    }
}
