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

package sleeper.statestore.s3;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.SplitFileReference;
import sleeper.core.statestore.SplitFileReferences;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileHasReferencesException;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceAlreadyExistsException;
import sleeper.core.statestore.exception.FileReferenceAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.statestore.exception.NewReferenceSameAsOldReferenceException;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.fileWithNoReferences;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.fileWithReferences;
import static sleeper.core.statestore.FilesReportTestHelper.activeFilesReport;
import static sleeper.core.statestore.FilesReportTestHelper.noFilesReport;
import static sleeper.core.statestore.FilesReportTestHelper.partialReadyForGCFilesReport;
import static sleeper.core.statestore.FilesReportTestHelper.readyForGCFilesReport;
import static sleeper.core.statestore.SplitFileReferenceRequest.splitFileToChildPartitions;

public class S3FileReferenceStoreIT extends S3StateStoreTestBase {

    private static final Instant DEFAULT_UPDATE_TIME = Instant.parse("2023-10-04T14:08:00Z");
    private static final Instant AFTER_DEFAULT_UPDATE_TIME = DEFAULT_UPDATE_TIME.plus(Duration.ofMinutes(2));
    private final Schema schema = schemaWithKey("key", new LongType());
    private final PartitionsBuilder partitions = new PartitionsBuilder(schema).singlePartition("root");
    private FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
    private StateStore store;

    @BeforeEach
    void setUpTable() throws StateStoreException {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, "1");
        store = new S3StateStore(instanceProperties, tableProperties, dynamoDBClient, new Configuration());
        store.fixTime(DEFAULT_UPDATE_TIME);
        store.initialise();
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
                    .hasEntrySatisfying("root", files ->
                            assertThat(files).containsExactlyInAnyOrder("file1", "file2", "file3"));
        }

        @Test
        void shouldSetLastUpdateTimeForFileWhenFixingTimeCorrectly() throws Exception {
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
        void shouldFailToAddSameFileTwice() throws Exception {
            // Given
            Instant updateTime = Instant.parse("2023-12-01T10:45:00Z");
            FileReference file = factory.rootFile("file1", 100L);
            store.fixTime(updateTime);
            store.addFile(file);

            // When / Then
            assertThatThrownBy(() -> store.addFile(file))
                    .isInstanceOf(FileReferenceAlreadyExistsException.class);

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
            assertThat(store.getAllFileReferencesWithMaxUnreferenced(100))
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
            assertThat(store.getAllFileReferencesWithMaxUnreferenced(100))
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
            assertThat(store.getAllFileReferencesWithMaxUnreferenced(100))
                    .isEqualTo(readyForGCFilesReport(updateTime, "test-file"));
            assertThat(store.getReadyForGCFilenamesBefore(updateTime.plus(Duration.ofDays(1))))
                    .containsExactly("test-file");
            assertThat(store.hasNoFiles()).isFalse();
        }
    }

    @Nested
    @DisplayName("Split file references across multiple partitions")
    class SplitFiles {
        @Test
        void shouldSplitOneFileInRootPartition() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference file = factory.rootFile("file", 100L);
            store.addFile(file);

            // When
            SplitFileReferences.from(store).split();

            // Then
            List<FileReference> expectedReferences = List.of(splitFile(file, "L"), splitFile(file, "R"));
            assertThat(store.getFileReferences())
                    .containsExactlyInAnyOrderElementsOf(expectedReferences);
            assertThat(store.getAllFileReferencesWithMaxUnreferenced(100))
                    .isEqualTo(activeFilesReport(DEFAULT_UPDATE_TIME, expectedReferences));
        }

        @Test
        void shouldSplitTwoFilesInOnePartition() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference file1 = factory.rootFile("file1", 100L);
            FileReference file2 = factory.rootFile("file2", 100L);
            store.addFiles(List.of(file1, file2));

            // When
            SplitFileReferences.from(store).split();

            // Then
            List<FileReference> expectedReferences = List.of(
                    splitFile(file1, "L"),
                    splitFile(file1, "R"),
                    splitFile(file2, "L"),
                    splitFile(file2, "R"));
            assertThat(store.getFileReferences())
                    .containsExactlyInAnyOrderElementsOf(expectedReferences);
            assertThat(store.getAllFileReferencesWithMaxUnreferenced(100))
                    .isEqualTo(activeFilesReport(DEFAULT_UPDATE_TIME, expectedReferences));
        }

        @Test
        void shouldSplitOneFileFromTwoOriginalPartitions() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            splitPartition("L", "LL", "LR", 2);
            splitPartition("R", "RL", "RR", 7);
            FileReference file = factory.rootFile("file", 100L);
            FileReference leftFile = splitFile(file, "L");
            FileReference rightFile = splitFile(file, "R");
            store.addFiles(List.of(leftFile, rightFile));

            // When
            SplitFileReferences.from(store).split();

            // Then
            List<FileReference> expectedReferences = List.of(
                    splitFile(leftFile, "LL"),
                    splitFile(leftFile, "LR"),
                    splitFile(rightFile, "RL"),
                    splitFile(rightFile, "RR"));
            assertThat(store.getFileReferences())
                    .containsExactlyInAnyOrderElementsOf(expectedReferences);
            assertThat(store.getAllFileReferencesWithMaxUnreferenced(100))
                    .isEqualTo(activeFilesReport(DEFAULT_UPDATE_TIME, expectedReferences));
        }

        @Test
        void shouldSplitFilesInDifferentPartitions() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            splitPartition("L", "LL", "LR", 2);
            splitPartition("R", "RL", "RR", 7);
            FileReference file1 = factory.partitionFile("L", "file1", 100L);
            FileReference file2 = factory.partitionFile("R", "file2", 200L);
            store.addFiles(List.of(file1, file2));

            // When
            SplitFileReferences.from(store).split();

            // Then
            List<FileReference> expectedReferences = List.of(
                    splitFile(file1, "LL"),
                    splitFile(file1, "LR"),
                    splitFile(file2, "RL"),
                    splitFile(file2, "RR"));
            assertThat(store.getFileReferences())
                    .containsExactlyInAnyOrderElementsOf(expectedReferences);
            assertThat(store.getAllFileReferencesWithMaxUnreferenced(100))
                    .isEqualTo(activeFilesReport(DEFAULT_UPDATE_TIME, expectedReferences));
        }

        @Test
        void shouldOnlyPerformOneLevelOfSplits() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5L);
            splitPartition("L", "LL", "LR", 2L);
            splitPartition("R", "RL", "RR", 7L);
            FileReference file = factory.rootFile("file.parquet", 100L);
            store.addFile(file);

            // When
            SplitFileReferences.from(store).split();

            // Then
            List<FileReference> expectedReferences = List.of(
                    splitFile(file, "L"),
                    splitFile(file, "R"));
            assertThat(store.getFileReferences())
                    .containsExactlyInAnyOrderElementsOf(expectedReferences);
            assertThat(store.getAllFileReferencesWithMaxUnreferenced(100))
                    .isEqualTo(activeFilesReport(DEFAULT_UPDATE_TIME, expectedReferences));
        }

        @Test
        void shouldNotSplitOneFileInLeafPartition() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5L);
            FileReference file = factory.partitionFile("L", "already-split.parquet", 100L);
            store.addFile(file);

            // When
            SplitFileReferences.from(store).split();

            // Then
            assertThat(store.getFileReferences())
                    .containsExactly(file);
            assertThat(store.getAllFileReferencesWithMaxUnreferenced(100))
                    .isEqualTo(activeFilesReport(DEFAULT_UPDATE_TIME, file));
        }

        @Test
        void shouldDoNothingWhenNoFilesExist() throws StateStoreException {
            // Given
            splitPartition("root", "L", "R", 5);

            // When
            SplitFileReferences.from(store).split();

            // Then
            assertThat(store.getFileReferences()).isEmpty();
            assertThat(store.getAllFileReferencesWithMaxUnreferenced(100))
                    .isEqualTo(noFilesReport());
        }

        @Test
        void shouldFailToSplitFileWhichDoesNotExist() throws StateStoreException {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference file = factory.rootFile("file", 100L);

            // When / Then
            assertThatThrownBy(() ->
                    store.splitFileReferences(List.of(
                            splitFileToChildPartitions(file, "L", "R"))))
                    .isInstanceOf(FileNotFoundException.class);
            assertThat(store.getFileReferences()).isEmpty();
            assertThat(store.getAllFileReferencesWithMaxUnreferenced(100))
                    .isEqualTo(noFilesReport());
        }

        @Test
        void shouldFailToSplitFileWhenReferenceDoesNotExistInPartition() throws StateStoreException {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference file = factory.rootFile("file", 100L);
            FileReference existingReference = splitFile(file, "L");
            store.addFile(existingReference);

            // When / Then
            assertThatThrownBy(() ->
                    store.splitFileReferences(List.of(
                            splitFileToChildPartitions(file, "L", "R"))))
                    .isInstanceOf(FileReferenceNotFoundException.class);
            assertThat(store.getFileReferences()).containsExactly(existingReference);
            assertThat(store.getAllFileReferencesWithMaxUnreferenced(100))
                    .isEqualTo(activeFilesReport(DEFAULT_UPDATE_TIME, existingReference));
        }

        @Test
        void shouldFailToSplitFileWhenTheSameFileWasAddedBackToParentPartition() throws StateStoreException {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference file = factory.rootFile("file", 100L);
            store.addFile(file);
            SplitFileReferences.from(store).split();
            // Ideally this would fail as the file is already referenced in partitions below it,
            // but not all state stores may be able to implement that
            store.addFile(file);

            // When / Then
            assertThatThrownBy(() -> SplitFileReferences.from(store).split())
                    .isInstanceOf(FileReferenceAlreadyExistsException.class);
            List<FileReference> expectedReferences = List.of(
                    file,
                    splitFile(file, "L"),
                    splitFile(file, "R"));
            assertThat(store.getFileReferences()).containsExactlyInAnyOrderElementsOf(expectedReferences);
            assertThat(store.getAllFileReferencesWithMaxUnreferenced(100))
                    .isEqualTo(activeFilesReport(DEFAULT_UPDATE_TIME, expectedReferences));
        }

        @Test
        void shouldThrowExceptionWhenSplittingFileHasBeenAssignedToTheJob() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference file = factory.rootFile("file", 100L);
            store.addFile(file);
            store.atomicallyAssignJobIdToFileReferences("job1", List.of(file));

            // When / Then
            assertThatThrownBy(() -> store.splitFileReferences(List.of(splitFileToChildPartitions(file, "L", "R"))))
                    .isInstanceOf(FileReferenceAssignedToJobException.class);
            assertThat(store.getFileReferences())
                    .containsExactly(file.toBuilder().jobId("job1").build());
            assertThat(store.getAllFileReferencesWithMaxUnreferenced(100))
                    .isEqualTo(activeFilesReport(DEFAULT_UPDATE_TIME, file.toBuilder().jobId("job1").build()));
        }
    }

    @Nested
    @DisplayName("Create compaction jobs")
    class CreateCompactionJobs {

        @Test
        public void shouldMarkFileWithJobId() throws Exception {
            // Given
            FileReference file = factory.rootFile("file", 100L);
            store.addFile(file);

            // When
            store.atomicallyAssignJobIdToFileReferences("job", Collections.singletonList(file));

            // Then
            assertThat(store.getFileReferences()).containsExactly(file.toBuilder().jobId("job").build());
            assertThat(store.getFileReferencesWithNoJobId()).isEmpty();
        }

        @Test
        public void shouldMarkOneHalfOfSplitFileWithJobId() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference file = factory.rootFile("file", 100L);
            FileReference left = splitFile(file, "L");
            FileReference right = splitFile(file, "R");
            store.addFiles(List.of(left, right));

            // When
            store.atomicallyAssignJobIdToFileReferences("job", Collections.singletonList(left));

            // Then
            assertThat(store.getFileReferences()).containsExactlyInAnyOrder(left.toBuilder().jobId("job").build(), right);
            assertThat(store.getFileReferencesWithNoJobId()).containsExactly(right);
        }

        @Test
        public void shouldNotMarkFileWithJobIdWhenOneIsAlreadySet() throws Exception {
            // Given
            FileReference file = factory.rootFile("file", 100L);
            store.addFile(file);
            store.atomicallyAssignJobIdToFileReferences("job1", Collections.singletonList(file));

            // When / Then
            assertThatThrownBy(() -> store.atomicallyAssignJobIdToFileReferences("job2", Collections.singletonList(file)))
                    .isInstanceOf(FileReferenceAssignedToJobException.class);
            assertThat(store.getFileReferences()).containsExactly(file.toBuilder().jobId("job1").build());
            assertThat(store.getFileReferencesWithNoJobId()).isEmpty();
        }

        @Test
        public void shouldNotUpdateOtherFilesIfOneFileAlreadyHasJobId() throws Exception {
            // Given
            FileReference file1 = factory.rootFile("file1", 100L);
            FileReference file2 = factory.rootFile("file2", 100L);
            FileReference file3 = factory.rootFile("file3", 100L);
            store.addFiles(Arrays.asList(file1, file2, file3));
            store.atomicallyAssignJobIdToFileReferences("job1", Collections.singletonList(file2));

            // When / Then
            assertThatThrownBy(() -> store.atomicallyAssignJobIdToFileReferences("job2", Arrays.asList(file1, file2, file3)))
                    .isInstanceOf(FileReferenceAssignedToJobException.class);
            assertThat(store.getFileReferences()).containsExactlyInAnyOrder(
                    file1, file2.toBuilder().jobId("job1").build(), file3);
            assertThat(store.getFileReferencesWithNoJobId()).containsExactlyInAnyOrder(file1, file3);
        }

        @Test
        public void shouldNotMarkFileWithJobIdWhenFileDoesNotExist() throws Exception {
            // Given
            FileReference file = factory.rootFile("existingFile", 100L);
            FileReference requested = factory.rootFile("requestedFile", 100L);
            store.addFile(file);

            // When / Then
            assertThatThrownBy(() -> store.atomicallyAssignJobIdToFileReferences("job", List.of(requested)))
                    .isInstanceOf(FileReferenceNotFoundException.class);
            assertThat(store.getFileReferences()).containsExactly(file);
            assertThat(store.getFileReferencesWithNoJobId()).containsExactly(file);
        }

        @Test
        public void shouldNotMarkFileWithJobIdWhenFileDoesNotExistAndStoreIsEmpty() throws Exception {
            // Given
            FileReference file = factory.rootFile("file", 100L);

            // When / Then
            assertThatThrownBy(() -> store.atomicallyAssignJobIdToFileReferences("job", List.of(file)))
                    .isInstanceOf(FileReferenceNotFoundException.class);
            assertThat(store.getFileReferences()).isEmpty();
            assertThat(store.getFileReferencesWithNoJobId()).isEmpty();
        }

        @Test
        public void shouldNotMarkFileWithJobIdWhenReferenceDoesNotExistInPartition() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference file = factory.rootFile("file", 100L);
            FileReference existingReference = splitFile(file, "L");
            store.addFile(existingReference);

            // When / Then
            assertThatThrownBy(() -> store.atomicallyAssignJobIdToFileReferences("job", List.of(file)))
                    .isInstanceOf(FileReferenceNotFoundException.class);
            assertThat(store.getFileReferences()).containsExactly(existingReference);
            assertThat(store.getFileReferencesWithNoJobId()).containsExactly(existingReference);
        }
    }

    @Nested
    @DisplayName("Apply compaction")
    class ApplyCompaction {

        @Test
        public void shouldSetFileReadyForGC() throws Exception {
            // Given
            FileReference oldFile = factory.rootFile("oldFile", 100L);
            FileReference newFile = factory.rootFile("newFile", 100L);
            store.addFile(oldFile);

            // When
            store.atomicallyAssignJobIdToFileReferences("job1", List.of(oldFile));
            store.atomicallyReplaceFileReferencesWithNewOne("job1", "root", List.of("oldFile"), newFile);

            // Then
            assertThat(store.getFileReferences()).containsExactly(newFile);
            assertThat(store.getFileReferencesWithNoJobId()).containsExactly(newFile);
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME))
                    .containsExactly("oldFile");
            assertThat(store.getPartitionToReferencedFilesMap())
                    .containsOnlyKeys("root")
                    .hasEntrySatisfying("root", files ->
                            assertThat(files).containsExactly("newFile"));
        }

        @Test
        void shouldFailToSetReadyForGCWhenAlreadyReadyForGC() throws Exception {
            // Given
            FileReference oldFile = factory.rootFile("oldFile", 100L);
            FileReference newFile = factory.rootFile("newFile", 100L);
            store.addFile(oldFile);

            // When
            store.atomicallyAssignJobIdToFileReferences("job1", List.of(oldFile));
            store.atomicallyReplaceFileReferencesWithNewOne("job1", "root", List.of("oldFile"), newFile);

            // Then
            assertThatThrownBy(() -> store.atomicallyReplaceFileReferencesWithNewOne("job1", "root", List.of("oldFile"), newFile))
                    .isInstanceOf(FileReferenceNotFoundException.class);
            assertThat(store.getFileReferences()).containsExactly(newFile);
            assertThat(store.getFileReferencesWithNoJobId()).containsExactly(newFile);
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME))
                    .containsExactly("oldFile");
            assertThat(store.getPartitionToReferencedFilesMap())
                    .containsOnlyKeys("root")
                    .hasEntrySatisfying("root", files ->
                            assertThat(files).containsExactly("newFile"));
        }

        @Test
        void shouldFailWhenFilesToMarkAsReadyForGCAreNotAssignedToJob() throws Exception {
            // Given
            FileReference oldFile = factory.rootFile("oldFile", 100L);
            FileReference newFile = factory.rootFile("newFile", 100L);
            store.addFile(oldFile);

            // When / Then
            assertThatThrownBy(() -> store.atomicallyReplaceFileReferencesWithNewOne(
                    "job1", "root", List.of("oldFile"), newFile))
                    .isInstanceOf(FileReferenceNotAssignedToJobException.class);
        }

        @Test
        public void shouldFailToSetFileReadyForGCWhichDoesNotExist() throws Exception {
            // Given
            FileReference newFile = factory.rootFile("newFile", 100L);

            // When / Then
            assertThatThrownBy(() -> store.atomicallyReplaceFileReferencesWithNewOne(
                    "job1", "root", List.of("oldFile"), newFile))
                    .isInstanceOf(FileNotFoundException.class);
            assertThat(store.getFileReferences()).isEmpty();
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
        }

        @Test
        public void shouldFailToSetFilesReadyForGCWhenOneDoesNotExist() throws Exception {
            // Given
            FileReference oldFile1 = factory.rootFile("oldFile1", 100L);
            FileReference newFile = factory.rootFile("newFile", 100L);
            store.addFile(oldFile1);
            store.atomicallyAssignJobIdToFileReferences("job1", List.of(oldFile1));

            // When / Then
            assertThatThrownBy(() -> store.atomicallyReplaceFileReferencesWithNewOne(
                    "job1", "root", List.of("oldFile1", "oldFile2"), newFile))
                    .isInstanceOf(FileNotFoundException.class);
            assertThat(store.getFileReferences()).containsExactly(oldFile1.toBuilder().jobId("job1").build());
            assertThat(store.getFileReferencesWithNoJobId()).isEmpty();
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
        }

        @Test
        public void shouldFailToSetFileReadyForGCWhenReferenceDoesNotExistInPartition() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference file = factory.rootFile("file", 100L);
            FileReference existingReference = splitFile(file, "L");
            store.addFile(existingReference);

            // When / Then
            assertThatThrownBy(() -> store.atomicallyReplaceFileReferencesWithNewOne(
                    "job1", "root", List.of("file"), factory.rootFile("file2", 100L)))
                    .isInstanceOf(FileReferenceNotFoundException.class);
            assertThat(store.getFileReferences()).containsExactly(existingReference);
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
        }

        @Test
        void shouldThrowExceptionWhenFileToBeMarkedReadyForGCHasSameFileNameAsNewFile() throws Exception {
            // Given
            FileReference file = factory.rootFile("file1", 100L);
            store.addFile(file);
            store.atomicallyAssignJobIdToFileReferences("job1", List.of(file));

            // When / Then
            assertThatThrownBy(() -> store.atomicallyReplaceFileReferencesWithNewOne(
                    "job1", "root", List.of("file1"), file))
                    .isInstanceOf(NewReferenceSameAsOldReferenceException.class);
            assertThat(store.getFileReferences()).containsExactly(file.toBuilder().jobId("job1").build());
            assertThat(store.getFileReferencesWithNoJobId()).isEmpty();
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
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
            store.atomicallyAssignJobIdToFileReferences("job1", List.of(leftFile));
            store.atomicallyReplaceFileReferencesWithNewOne("job1", "L", List.of("splitFile"), compactionOutputFile);

            // When / Then
            assertThat(store.getReadyForGCFilenamesBefore(latestTimeForGc))
                    .isEmpty();
        }

        @Test
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
            store.atomicallyAssignJobIdToFileReferences("job1", List.of(leftFile));
            store.atomicallyReplaceFileReferencesWithNewOne("job1", "L", List.of("readyForGc"), leftOutputFile);
            store.atomicallyAssignJobIdToFileReferences("job2", List.of(rightFile));
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
            store.atomicallyAssignJobIdToFileReferences("job1", List.of(leftFile));
            store.atomicallyAssignJobIdToFileReferences("job2", List.of(rightFile));
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
    @DisplayName("Apply garbage collection")
    class ApplyGarbageCollection {

        @Test
        public void shouldDeleteGarbageCollectedFile() throws Exception {
            // Given
            FileReference oldFile = factory.rootFile("oldFile", 100L);
            FileReference newFile = factory.rootFile("newFile", 100L);
            store.addFile(oldFile);
            store.atomicallyAssignJobIdToFileReferences("job1", List.of(oldFile));
            store.atomicallyReplaceFileReferencesWithNewOne("job1", "root", List.of("oldFile"), newFile);

            // When
            store.deleteGarbageCollectedFileReferenceCounts(List.of("oldFile"));

            // Then
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
        }

        @Test
        void shouldDeleteGarbageCollectedFileSplitAcrossTwoPartitions() throws Exception {
            // Given we have partitions, input files and output files for compactions
            splitPartition("root", "L", "R", 5);
            FileReference rootFile = factory.rootFile("file", 100L);
            FileReference leftFile = splitFile(rootFile, "L");
            FileReference rightFile = splitFile(rootFile, "R");
            FileReference leftOutputFile = factory.partitionFile("L", "leftOutput", 100L);
            FileReference rightOutputFile = factory.partitionFile("R", "rightOutput", 100L);

            // And the file was ingested as two references, then compacted into each partition
            store.addFiles(List.of(leftFile, rightFile));
            store.atomicallyAssignJobIdToFileReferences("job1", List.of(leftFile));
            store.atomicallyReplaceFileReferencesWithNewOne("job1", "L", List.of("file"), leftOutputFile);
            store.atomicallyAssignJobIdToFileReferences("job2", List.of(rightFile));
            store.atomicallyReplaceFileReferencesWithNewOne("job2", "R", List.of("file"), rightOutputFile);

            // When
            store.deleteGarbageCollectedFileReferenceCounts(List.of("file"));

            // Then
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
            assertThat(store.getAllFileReferencesWithMaxUnreferenced(100))
                    .isEqualTo(activeFilesReport(DEFAULT_UPDATE_TIME, leftOutputFile, rightOutputFile));
        }

        @Test
        public void shouldFailToDeleteActiveFile() throws Exception {
            // Given
            FileReference file = factory.rootFile("test", 100L);
            store.addFile(file);

            // When / Then
            assertThatThrownBy(() -> store.deleteGarbageCollectedFileReferenceCounts(List.of("test")))
                    .isInstanceOf(FileHasReferencesException.class);
        }

        @Test
        public void shouldFailToDeleteFileWhichWasNotAdded() {
            // When / Then
            assertThatThrownBy(() -> store.deleteGarbageCollectedFileReferenceCounts(List.of("test")))
                    .isInstanceOf(FileNotFoundException.class);
        }

        @Test
        public void shouldFailToDeleteActiveFileWhenOneOfTwoSplitRecordsIsReadyForGC() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference rootFile = factory.rootFile("file", 100L);
            FileReference leftFile = splitFile(rootFile, "L");
            FileReference rightFile = splitFile(rootFile, "R");
            FileReference leftOutputFile = factory.partitionFile("L", "leftOutput", 100L);
            store.addFiles(List.of(leftFile, rightFile));
            store.atomicallyAssignJobIdToFileReferences("job1", List.of(leftFile));
            store.atomicallyReplaceFileReferencesWithNewOne("job1", "L", List.of("file"), leftOutputFile);

            // When / Then
            assertThatThrownBy(() -> store.deleteGarbageCollectedFileReferenceCounts(List.of("file")))
                    .isInstanceOf(FileHasReferencesException.class);
        }

        @Test
        public void shouldDeleteGarbageCollectedFileWhileIteratingThroughReadyForGCFiles() throws Exception {
            // Given
            FileReference oldFile1 = factory.rootFile("oldFile1", 100L);
            FileReference oldFile2 = factory.rootFile("oldFile2", 100L);
            FileReference newFile = factory.rootFile("newFile", 100L);
            store.addFiles(List.of(oldFile1, oldFile2));
            store.atomicallyAssignJobIdToFileReferences("job1", List.of(oldFile1, oldFile2));
            store.atomicallyReplaceFileReferencesWithNewOne(
                    "job1", "root", List.of("oldFile1", "oldFile2"), newFile);

            // When
            Iterator<String> iterator = store.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)).iterator();
            store.deleteGarbageCollectedFileReferenceCounts(List.of(iterator.next()));

            // Then
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME))
                    .containsExactly(iterator.next());
            assertThat(iterator).isExhausted();
        }

        @Test
        public void shouldFailToDeleteActiveFileWhenAlsoDeletingReadyForGCFile() throws Exception {
            // Given
            FileReference activeFile = factory.rootFile("activeFile", 100L);
            store.addFilesWithReferences(List.of(
                    fileWithNoReferences("gcFile"),
                    fileWithReferences(List.of(activeFile))));

            // When / Then
            assertThatThrownBy(() -> store.deleteGarbageCollectedFileReferenceCounts(List.of("gcFile", "activeFile")))
                    .isInstanceOf(FileHasReferencesException.class);
            assertThat(store.getFileReferences())
                    .containsExactly(activeFile);
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME))
                    .containsExactly("gcFile");
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
            AllReferencesToAllFiles report = store.getAllFileReferencesWithMaxUnreferenced(5);

            // Then
            assertThat(report).isEqualTo(activeFilesReport(DEFAULT_UPDATE_TIME, file));
        }

        @Test
        void shouldReportOneReadyForGCFile() throws Exception {
            // Given
            store.addFilesWithReferences(List.of(fileWithNoReferences("test")));

            // When
            AllReferencesToAllFiles report = store.getAllFileReferencesWithMaxUnreferenced(5);

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
            AllReferencesToAllFiles report = store.getAllFileReferencesWithMaxUnreferenced(5);

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
            AllReferencesToAllFiles report = store.getAllFileReferencesWithMaxUnreferenced(5);

            // Then
            assertThat(report).isEqualTo(activeFilesReport(DEFAULT_UPDATE_TIME, leftFile, rightFile));
        }

        @Test
        void shouldReportFileSplitOverTwoPartitionsWithOneSideCompacted() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference rootFile = factory.rootFile("file", 100L);
            FileReference leftFile = splitFile(rootFile, "L");
            FileReference rightFile = splitFile(rootFile, "R");
            FileReference outputFile = factory.partitionFile("L", 50L);
            store.addFiles(List.of(leftFile, rightFile));
            store.atomicallyAssignJobIdToFileReferences("job1", List.of(leftFile));
            store.atomicallyReplaceFileReferencesWithNewOne("job1", "L", List.of("file"), outputFile);

            // When
            AllReferencesToAllFiles report = store.getAllFileReferencesWithMaxUnreferenced(5);

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
            AllReferencesToAllFiles report = store.getAllFileReferencesWithMaxUnreferenced(2);

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
            AllReferencesToAllFiles report = store.getAllFileReferencesWithMaxUnreferenced(2);

            // Then
            assertThat(report).isEqualTo(readyForGCFilesReport(DEFAULT_UPDATE_TIME, "test1", "test2"));
        }
    }

    private void splitPartition(String parentId, String leftId, String rightId, long splitPoint) throws StateStoreException {
        partitions.splitToNewChildren(parentId, leftId, rightId, splitPoint)
                .applySplit(store, parentId);
        factory = FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
    }

    private FileReference splitFile(FileReference parentFile, String childPartitionId) {
        return SplitFileReference.referenceForChildPartition(parentFile, childPartitionId)
                .toBuilder().lastStateStoreUpdateTime(DEFAULT_UPDATE_TIME).build();
    }

    private static FileReference withLastUpdate(Instant updateTime, FileReference file) {
        return file.toBuilder().lastStateStoreUpdateTime(updateTime).build();
    }
}
