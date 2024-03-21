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
package sleeper.core.statestore.transactionlog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.exception.FileAlreadyExistsException;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.fileWithNoReferences;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.fileWithReferences;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.FileReferenceTestData.AFTER_DEFAULT_UPDATE_TIME;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.core.statestore.FileReferenceTestData.splitFile;
import static sleeper.core.statestore.FileReferenceTestData.withLastUpdate;
import static sleeper.core.statestore.FilesReportTestHelper.activeFilesReport;
import static sleeper.core.statestore.FilesReportTestHelper.partialReadyForGCFilesReport;
import static sleeper.core.statestore.FilesReportTestHelper.readyForGCFilesReport;

public class TransactionLogFileReferenceStoreTest extends InMemoryTransactionLogStateStoreTestBase {

    @BeforeEach
    void setUp() throws Exception {
        initialiseWithSchema(schemaWithKey("key", new LongType()));
    }

    @Nested
    @DisplayName("Handle ingest")
    class HandleIngest {

        @Test
        public void shouldAddAndReadActiveFiles() throws Exception {
            // Given
            Instant fixedUpdateTime = Instant.parse("2023-10-04T14:08:00Z");
            FileReference file1 = factory.rootFile("file1", 100L);
            FileReference file2 = factory.rootFile("file2", 100L);
            FileReference file3 = factory.rootFile("file3", 100L);

            // When
            store.fixTime(fixedUpdateTime);
            store.addFile(file1);
            store.addFiles(List.of(file2, file3));

            // Then
            assertThat(store.getFileReferences()).containsExactlyInAnyOrder(file1, file2, file3);
            assertThat(store.getFileReferencesWithNoJobId()).containsExactlyInAnyOrder(file1, file2, file3);
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
            assertThat(store.getPartitionToReferencedFilesMap())
                    .containsOnlyKeys("root")
                    .hasEntrySatisfying("root", files -> assertThat(files).containsExactlyInAnyOrder("file1", "file2", "file3"));
        }

        @Test
        void shouldSetLastUpdateTimeForFile() throws Exception {
            // Given
            Instant updateTime = Instant.parse("2023-12-01T10:45:00Z");
            FileReference file = factory.rootFile("file1", 100L);

            // When
            store.fixTime(updateTime);
            store.addFile(file);

            // Then
            assertThat(store.getFileReferences()).containsExactlyInAnyOrder(withLastUpdate(updateTime, file));
        }

        @Test
        void shouldAddFileSplitOverTwoPartitions() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            Instant updateTime = Instant.parse("2023-12-01T10:45:00Z");
            FileReference rootFile = factory.rootFile("file1", 100L);
            FileReference leftFile = splitFile(rootFile, "L");
            FileReference rightFile = splitFile(rootFile, "R");
            store.fixTime(updateTime);
            store.addFiles(List.of(leftFile, rightFile));

            // When / Then
            assertThat(store.getFileReferences()).containsExactlyInAnyOrder(
                    withLastUpdate(updateTime, leftFile),
                    withLastUpdate(updateTime, rightFile));
        }

        @Test
        void shouldAddFileWithReferencesSplitOverTwoPartitions() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            Instant updateTime = Instant.parse("2023-12-01T10:45:00Z");
            FileReference rootFile = factory.rootFile("file1", 100L);
            FileReference leftFile = splitFile(rootFile, "L");
            FileReference rightFile = splitFile(rootFile, "R");
            store.fixTime(updateTime);
            store.addFilesWithReferences(List.of(fileWithReferences(List.of(leftFile, rightFile))));

            // When / Then
            assertThat(store.getFileReferences()).containsExactlyInAnyOrder(
                    withLastUpdate(updateTime, leftFile),
                    withLastUpdate(updateTime, rightFile));
            assertThat(store.getAllFilesWithMaxUnreferenced(100))
                    .isEqualTo(activeFilesReport(updateTime, leftFile, rightFile));
            assertThat(store.getReadyForGCFilenamesBefore(updateTime.plus(Duration.ofDays(1))))
                    .isEmpty();
            assertThat(store.hasNoFiles()).isFalse();
        }

        @Test
        void shouldAddTwoFilesWithReferences() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            Instant updateTime = Instant.parse("2023-12-01T10:45:00Z");
            FileReference file1 = factory.rootFile("file1", 100L);
            FileReference leftFile1 = splitFile(file1, "L");
            FileReference rightFile1 = splitFile(file1, "R");
            FileReference file2 = factory.rootFile("file2", 100L);
            store.fixTime(updateTime);
            store.addFilesWithReferences(List.of(
                    fileWithReferences(List.of(leftFile1, rightFile1)),
                    fileWithReferences(List.of(file2))));

            // When / Then
            assertThat(store.getFileReferences()).containsExactlyInAnyOrder(
                    withLastUpdate(updateTime, leftFile1),
                    withLastUpdate(updateTime, rightFile1),
                    withLastUpdate(updateTime, file2));
            assertThat(store.getAllFilesWithMaxUnreferenced(100))
                    .isEqualTo(activeFilesReport(updateTime, leftFile1, rightFile1, file2));
            assertThat(store.getReadyForGCFilenamesBefore(updateTime.plus(Duration.ofDays(1))))
                    .isEmpty();
            assertThat(store.hasNoFiles()).isFalse();
        }

        @Test
        void shouldAddFileWithNoReferencesForGC() throws Exception {
            // Given
            Instant updateTime = Instant.parse("2023-12-01T10:45:00Z");
            store.fixTime(updateTime);
            store.addFilesWithReferences(List.of(fileWithNoReferences("test-file")));

            // When / Then
            assertThat(store.getFileReferences()).isEmpty();
            assertThat(store.getAllFilesWithMaxUnreferenced(100))
                    .isEqualTo(readyForGCFilesReport(updateTime, "test-file"));
            assertThat(store.getReadyForGCFilenamesBefore(updateTime.plus(Duration.ofDays(1))))
                    .containsExactly("test-file");
            assertThat(store.hasNoFiles()).isFalse();
        }

        @Test
        @Disabled("TODO")
        void shouldFailToAddSameFileTwice() throws Exception {
            // Given
            Instant updateTime = Instant.parse("2023-12-01T10:45:00Z");
            FileReference file = factory.rootFile("file1", 100L);
            store.fixTime(updateTime);
            store.addFile(file);

            // When / Then
            assertThatThrownBy(() -> store.addFile(file))
                    .isInstanceOf(FileAlreadyExistsException.class);
            assertThat(store.getFileReferences()).containsExactlyInAnyOrder(withLastUpdate(updateTime, file));
            assertThatThrownBy(() -> store.addFilesWithReferences(List.of(fileWithReferences(file))))
                    .isInstanceOf(FileAlreadyExistsException.class);
            assertThat(store.getFileReferences()).containsExactlyInAnyOrder(withLastUpdate(updateTime, file));
        }

        @Test
        @Disabled("TODO")
        void shouldFailToAddAnotherReferenceForSameFile() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference file = factory.rootFile("file1", 100L);
            FileReference leftFile = splitFile(file, "L");
            FileReference rightFile = splitFile(file, "R");
            store.addFile(leftFile);

            // When / Then
            assertThatThrownBy(() -> store.addFile(rightFile))
                    .isInstanceOf(FileAlreadyExistsException.class);
            assertThat(store.getFileReferences()).containsExactly(leftFile);
            assertThatThrownBy(() -> store.addFilesWithReferences(List.of(fileWithReferences(rightFile))))
                    .isInstanceOf(FileAlreadyExistsException.class);
            assertThat(store.getFileReferences()).containsExactly(leftFile);
        }
    }

    @Nested
    @DisplayName("Find files for garbage collection")
    class FindFilesForGarbageCollection {

        @Test
        public void shouldFindFileWithNoReferencesWhichWasUpdatedLongEnoughAgo() throws Exception {
            // Given
            Instant updateTime = Instant.parse("2023-10-04T14:08:00Z");
            Instant latestTimeForGc = Instant.parse("2023-10-04T14:09:00Z");
            store.fixTime(updateTime);
            store.addFilesWithReferences(List.of(fileWithNoReferences("readyForGc")));

            // When / Then
            assertThat(store.getReadyForGCFilenamesBefore(latestTimeForGc))
                    .containsExactly("readyForGc");
        }

        @Test
        public void shouldNotFindFileWhichWasMarkedReadyForGCTooRecently() throws Exception {
            // Given
            Instant updateTime = Instant.parse("2023-10-04T14:08:00Z");
            Instant latestTimeForGc = Instant.parse("2023-10-04T14:07:00Z");
            store.fixTime(updateTime);
            store.addFilesWithReferences(List.of(fileWithNoReferences("readyForGc")));

            // When / Then
            assertThat(store.getReadyForGCFilenamesBefore(latestTimeForGc))
                    .isEmpty();
        }

        @Test
        public void shouldNotFindFileWhichHasTwoReferencesAndOnlyOneWasMarkedAsReadyForGC() throws Exception {
            // Given
            Instant updateTime = Instant.parse("2023-10-04T14:08:00Z");
            Instant latestTimeForGc = Instant.parse("2023-10-04T14:09:00Z");
            splitPartition("root", "L", "R", 5);
            FileReference rootFile = factory.rootFile("splitFile", 100L);
            FileReference leftFile = splitFile(rootFile, "L");
            FileReference rightFile = splitFile(rootFile, "R");
            FileReference compactionOutputFile = factory.partitionFile("L", "compactedFile", 100L);
            store.fixTime(updateTime);
            store.addFiles(List.of(leftFile, rightFile));
            store.assignJobIds(List.of(
                    assignJobOnPartitionToFiles("job1", "L", List.of("splitFile"))));
            store.atomicallyReplaceFileReferencesWithNewOne("job1", "L", List.of("splitFile"), compactionOutputFile);

            // When / Then
            assertThat(store.getReadyForGCFilenamesBefore(latestTimeForGc))
                    .isEmpty();
        }

        @Test
        @Disabled("TODO")
        public void shouldFindFileWhichHasTwoReferencesAndBothWereMarkedAsReadyForGC() throws Exception {
            // Given
            Instant updateTime = Instant.parse("2023-10-04T14:08:00Z");
            Instant latestTimeForGc = Instant.parse("2023-10-04T14:09:00Z");
            splitPartition("root", "L", "R", 5);
            FileReference rootFile = factory.rootFile("readyForGc", 100L);
            FileReference leftFile = splitFile(rootFile, "L");
            FileReference rightFile = splitFile(rootFile, "R");
            FileReference leftOutputFile = factory.partitionFile("L", "leftOutput", 100L);
            FileReference rightOutputFile = factory.partitionFile("R", "rightOutput", 100L);
            store.fixTime(updateTime);
            store.addFiles(List.of(leftFile, rightFile));
            store.assignJobIds(List.of(
                    assignJobOnPartitionToFiles("job1", "L", List.of("readyForGc")),
                    assignJobOnPartitionToFiles("job2", "R", List.of("readyForGc"))));
            store.atomicallyReplaceFileReferencesWithNewOne("job1", "L", List.of("readyForGc"), leftOutputFile);
            store.atomicallyReplaceFileReferencesWithNewOne("job2", "R", List.of("readyForGc"), rightOutputFile);

            // When / Then
            assertThat(store.getReadyForGCFilenamesBefore(latestTimeForGc))
                    .containsExactly("readyForGc");
        }

        @Test
        public void shouldNotFindSplitFileWhenOnlyFirstReadyForGCUpdateIsOldEnough() throws Exception {
            // Given ingest, compactions and GC check happened in order
            Instant ingestTime = Instant.parse("2023-10-04T14:08:00Z");
            Instant firstCompactionTime = Instant.parse("2023-10-04T14:09:00Z");
            Instant secondCompactionTime = Instant.parse("2023-10-04T14:10:00Z");
            Instant latestTimeForGc = Instant.parse("2023-10-04T14:09:30Z");

            // And we have partitions, input files and output files
            splitPartition("root", "L", "R", 5);
            FileReference rootFile = factory.rootFile("readyForGc", 100L);
            FileReference leftFile = splitFile(rootFile, "L");
            FileReference rightFile = splitFile(rootFile, "R");
            FileReference leftOutputFile = factory.partitionFile("L", "leftOutput", 100L);
            FileReference rightOutputFile = factory.partitionFile("R", "rightOutput", 100L);

            // And ingest and compactions happened at the expected times
            store.fixTime(ingestTime);
            store.addFiles(List.of(leftFile, rightFile));
            store.assignJobIds(List.of(
                    assignJobOnPartitionToFiles("job1", "L", List.of("readyForGc")),
                    assignJobOnPartitionToFiles("job2", "R", List.of("readyForGc"))));
            store.fixTime(firstCompactionTime);
            store.atomicallyReplaceFileReferencesWithNewOne("job1", "L", List.of("readyForGc"), leftOutputFile);
            store.fixTime(secondCompactionTime);
            store.atomicallyReplaceFileReferencesWithNewOne("job2", "R", List.of("readyForGc"), rightOutputFile);

            // When / Then
            assertThat(store.getReadyForGCFilenamesBefore(latestTimeForGc))
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("Report file status")
    class ReportFileStatus {

        @Test
        void shouldReportOneActiveFile() throws Exception {
            // Given
            FileReference file = factory.rootFile("test", 100L);
            store.addFile(file);

            // When
            AllReferencesToAllFiles report = store.getAllFilesWithMaxUnreferenced(5);

            // Then
            assertThat(report).isEqualTo(activeFilesReport(DEFAULT_UPDATE_TIME, file));
        }

        @Test
        void shouldReportOneReadyForGCFile() throws Exception {
            // Given
            store.addFilesWithReferences(List.of(fileWithNoReferences("test")));

            // When
            AllReferencesToAllFiles report = store.getAllFilesWithMaxUnreferenced(5);

            // Then
            assertThat(report).isEqualTo(readyForGCFilesReport(DEFAULT_UPDATE_TIME, "test"));
        }

        @Test
        void shouldReportTwoActiveFiles() throws Exception {
            // Given
            FileReference file1 = factory.rootFile("file1", 100L);
            FileReference file2 = factory.rootFile("file2", 100L);
            store.addFiles(List.of(file1, file2));

            // When
            AllReferencesToAllFiles report = store.getAllFilesWithMaxUnreferenced(5);

            // Then
            assertThat(report).isEqualTo(activeFilesReport(DEFAULT_UPDATE_TIME, file1, file2));
        }

        @Test
        void shouldReportFileSplitOverTwoPartitions() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference rootFile = factory.rootFile("file", 100L);
            FileReference leftFile = splitFile(rootFile, "L");
            FileReference rightFile = splitFile(rootFile, "R");
            store.addFiles(List.of(leftFile, rightFile));

            // When
            AllReferencesToAllFiles report = store.getAllFilesWithMaxUnreferenced(5);

            // Then
            assertThat(report).isEqualTo(activeFilesReport(DEFAULT_UPDATE_TIME, leftFile, rightFile));
        }

        @Test
        @Disabled("TODO")
        void shouldReportFileSplitOverTwoPartitionsWithOneSideCompacted() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference rootFile = factory.rootFile("file", 100L);
            FileReference leftFile = splitFile(rootFile, "L");
            FileReference rightFile = splitFile(rootFile, "R");
            FileReference outputFile = factory.partitionFile("L", 50L);
            store.addFiles(List.of(leftFile, rightFile));
            store.assignJobIds(List.of(
                    assignJobOnPartitionToFiles("job1", "L", List.of("file"))));
            store.atomicallyReplaceFileReferencesWithNewOne("job1", "L", List.of("file"), outputFile);

            // When
            AllReferencesToAllFiles report = store.getAllFilesWithMaxUnreferenced(5);

            // Then
            assertThat(report).isEqualTo(activeFilesReport(DEFAULT_UPDATE_TIME, outputFile, rightFile));
        }

        @Test
        void shouldReportReadyForGCFilesWithLimit() throws Exception {
            // Given
            store.addFilesWithReferences(List.of(
                    fileWithNoReferences("test1"),
                    fileWithNoReferences("test2"),
                    fileWithNoReferences("test3")));

            // When
            AllReferencesToAllFiles report = store.getAllFilesWithMaxUnreferenced(2);

            // Then
            assertThat(report).isEqualTo(partialReadyForGCFilesReport(DEFAULT_UPDATE_TIME, "test1", "test2"));
        }

        @Test
        void shouldReportReadyForGCFilesMeetingLimit() throws Exception {
            // Given
            store.addFilesWithReferences(List.of(
                    fileWithNoReferences("test1"),
                    fileWithNoReferences("test2")));

            // When
            AllReferencesToAllFiles report = store.getAllFilesWithMaxUnreferenced(2);

            // Then
            assertThat(report).isEqualTo(readyForGCFilesReport(DEFAULT_UPDATE_TIME, "test1", "test2"));
        }
    }

    @Nested
    @DisplayName("Get files by partition")
    class FilesByPartition {

        @Test
        public void shouldReturnMultipleFilesOnEachPartition() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference rootFile1 = factory.rootFile("rootFile1", 10);
            FileReference rootFile2 = factory.rootFile("rootFile2", 10);
            FileReference leftFile1 = factory.partitionFile("L", "leftFile1", 10);
            FileReference leftFile2 = factory.partitionFile("L", "leftFile2", 10);
            FileReference rightFile1 = factory.partitionFile("R", "rightFile1", 10);
            FileReference rightFile2 = factory.partitionFile("R", "rightFile2", 10);
            store.addFiles(List.of(rootFile1, rootFile2, leftFile1, leftFile2, rightFile1, rightFile2));

            // When / Then
            assertThat(store.getPartitionToReferencedFilesMap())
                    .containsOnlyKeys("root", "L", "R")
                    .hasEntrySatisfying("root", values -> assertThat(values)
                            .containsExactlyInAnyOrder("rootFile1", "rootFile2"))
                    .hasEntrySatisfying("L", values -> assertThat(values)
                            .containsExactlyInAnyOrder("leftFile1", "leftFile2"))
                    .hasEntrySatisfying("R", values -> assertThat(values)
                            .containsExactlyInAnyOrder("rightFile1", "rightFile2"));
        }

        @Test
        public void shouldNotReturnPartitionsWithNoFiles() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference file = factory.partitionFile("L", "file", 100);
            store.addFile(file);

            // When / Then
            assertThat(store.getPartitionToReferencedFilesMap())
                    .isEqualTo(Map.of("L", List.of("file")));
        }
    }
}
