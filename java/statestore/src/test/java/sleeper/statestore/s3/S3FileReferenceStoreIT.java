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
import sleeper.core.statestore.AllFileReferences;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.SplitFileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.FilesReportTestHelper.activeFilesReport;
import static sleeper.core.statestore.FilesReportTestHelper.partialReadyForGCFilesReport;
import static sleeper.core.statestore.FilesReportTestHelper.readyForGCFilesReport;

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
            assertThat(store.getActiveFiles()).containsExactlyInAnyOrder(file1, file2, file3);
            assertThat(store.getActiveFilesWithNoJobId()).containsExactlyInAnyOrder(file1, file2, file3);
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
            assertThat(store.getPartitionToActiveFilesMap())
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
            assertThat(store.getActiveFiles()).containsExactlyInAnyOrder(withLastUpdate(updateTime, file));
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
                    .isInstanceOf(StateStoreException.class);
            assertThat(store.getActiveFiles()).containsExactlyInAnyOrder(withLastUpdate(updateTime, file));
        }

        @Test
        void shouldAddReferenceToFile() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            Instant updateTime = Instant.parse("2023-12-01T10:45:00Z");
            FileReference file = factory.rootFile("file1", 100L);
            FileReference reference = splitFile(file, "L");
            store.fixTime(updateTime);
            store.addFile(file);
            store.addFile(reference);

            // When / Then
            assertThat(store.getActiveFiles()).containsExactlyInAnyOrder(
                    withLastUpdate(updateTime, file),
                    withLastUpdate(updateTime, reference));
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
            store.atomicallyUpdateJobStatusOfFiles("job", Collections.singletonList(file));

            // Then
            assertThat(store.getActiveFiles()).containsExactly(file.toBuilder().jobId("job").build());
            assertThat(store.getActiveFilesWithNoJobId()).isEmpty();
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
            store.atomicallyUpdateJobStatusOfFiles("job", Collections.singletonList(left));

            // Then
            assertThat(store.getActiveFiles()).containsExactly(left.toBuilder().jobId("job").build(), right);
            assertThat(store.getActiveFilesWithNoJobId()).containsExactly(right);
        }

        @Test
        public void shouldNotMarkFileWithJobIdWhenOneIsAlreadySet() throws Exception {
            // Given
            FileReference file = factory.rootFile("file", 100L);
            store.addFile(file);
            store.atomicallyUpdateJobStatusOfFiles("job1", Collections.singletonList(file));

            // When / Then
            assertThatThrownBy(() -> store.atomicallyUpdateJobStatusOfFiles("job2", Collections.singletonList(file)))
                    .isInstanceOf(StateStoreException.class);
            assertThat(store.getActiveFiles()).containsExactly(file.toBuilder().jobId("job1").build());
            assertThat(store.getActiveFilesWithNoJobId()).isEmpty();
        }

        @Test
        public void shouldNotUpdateOtherFilesIfOneFileAlreadyHasJobId() throws Exception {
            // Given
            FileReference file1 = factory.rootFile("file1", 100L);
            FileReference file2 = factory.rootFile("file2", 100L);
            FileReference file3 = factory.rootFile("file3", 100L);
            store.addFiles(Arrays.asList(file1, file2, file3));
            store.atomicallyUpdateJobStatusOfFiles("job1", Collections.singletonList(file2));

            // When / Then
            assertThatThrownBy(() -> store.atomicallyUpdateJobStatusOfFiles("job2", Arrays.asList(file1, file2, file3)))
                    .isInstanceOf(StateStoreException.class);
            assertThat(store.getActiveFiles()).containsExactlyInAnyOrder(
                    file1, file2.toBuilder().jobId("job1").build(), file3);
            assertThat(store.getActiveFilesWithNoJobId()).containsExactlyInAnyOrder(file1, file3);
        }

        @Test
        public void shouldNotMarkFileWithJobIdWhenFileDoesNotExist() throws Exception {
            // Given
            FileReference file = factory.rootFile("existingFile", 100L);
            FileReference requested = factory.rootFile("requestedFile", 100L);
            store.addFile(file);

            // When / Then
            assertThatThrownBy(() -> store.atomicallyUpdateJobStatusOfFiles("job", List.of(requested)))
                    .isInstanceOf(StateStoreException.class);
            assertThat(store.getActiveFiles()).containsExactly(file);
            assertThat(store.getActiveFilesWithNoJobId()).containsExactly(file);
        }

        @Test
        public void shouldNotMarkFileWithJobIdWhenFileDoesNotExistAndStoreIsEmpty() throws Exception {
            // Given
            FileReference file = factory.rootFile("file", 100L);

            // When / Then
            assertThatThrownBy(() -> store.atomicallyUpdateJobStatusOfFiles("job", List.of(file)))
                    .isInstanceOf(StateStoreException.class);
            assertThat(store.getActiveFiles()).isEmpty();
            assertThat(store.getActiveFilesWithNoJobId()).isEmpty();
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
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("root", List.of("oldFile"), List.of(newFile));

            // Then
            assertThat(store.getActiveFiles()).containsExactly(newFile);
            assertThat(store.getActiveFilesWithNoJobId()).containsExactly(newFile);
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME))
                    .containsExactly("oldFile");
            assertThat(store.getPartitionToActiveFilesMap())
                    .containsOnlyKeys("root")
                    .hasEntrySatisfying("root", files ->
                            assertThat(files).containsExactly("newFile"));
        }

        @Test
        void shouldSplitFileByReferenceAcrossTwoPartitions() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference rootFile = factory.rootFile("file", 100L);
            FileReference leftFile = splitFile(rootFile, "L");
            FileReference rightFile = splitFile(rootFile, "R");
            store.addFile(rootFile);

            // When
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("root", List.of("file"), List.of(leftFile, rightFile));

            // Then
            assertThat(store.getActiveFiles()).containsExactlyInAnyOrder(leftFile, rightFile);
            assertThat(store.getActiveFilesWithNoJobId()).containsExactlyInAnyOrder(leftFile, rightFile);
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
            assertThat(store.getPartitionToActiveFilesMap())
                    .isEqualTo(Map.of("L", List.of("file"), "R", List.of("file")));
        }

        @Test
        void shouldSplitFileByCopyAcrossTwoPartitions() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference rootFile = factory.rootFile("file", 100L);
            FileReference leftFile = splitFileByCopy(rootFile, "L", "file2");
            FileReference rightFile = splitFileByCopy(rootFile, "R", "file2");
            store.addFile(rootFile);

            // When
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("root", List.of("file"), List.of(leftFile, rightFile));

            // Then
            assertThat(store.getActiveFiles()).containsExactlyInAnyOrder(leftFile, rightFile);
            assertThat(store.getActiveFilesWithNoJobId()).containsExactlyInAnyOrder(leftFile, rightFile);
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME))
                    .containsExactly("file");
            assertThat(store.getPartitionToActiveFilesMap())
                    .isEqualTo(Map.of("L", List.of("file2"), "R", List.of("file2")));
        }

        @Test
        void shouldFailToSetReadyForGCWhenAlreadyReadyForGC() throws Exception {
            // Given
            FileReference oldFile = factory.rootFile("oldFile", 100L);
            FileReference newFile = factory.rootFile("newFile", 100L);
            store.addFile(oldFile);

            // When
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("root", List.of("oldFile"), List.of(newFile));

            // Then
            assertThatThrownBy(() -> store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("root", List.of("oldFile"), List.of(newFile)))
                    .isInstanceOf(StateStoreException.class);
            assertThat(store.getActiveFiles()).containsExactly(newFile);
            assertThat(store.getActiveFilesWithNoJobId()).containsExactly(newFile);
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME))
                    .containsExactly("oldFile");
            assertThat(store.getPartitionToActiveFilesMap())
                    .containsOnlyKeys("root")
                    .hasEntrySatisfying("root", files ->
                            assertThat(files).containsExactly("newFile"));
        }

        @Test
        public void shouldStillHaveAFileAfterSettingOnlyFileReadyForGC() throws Exception {
            // Given
            FileReference file = factory.rootFile("file", 100L);
            store.addFile(file);

            // When
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("root", List.of("file"), List.of());

            // Then
            assertThat(store.getActiveFiles()).isEmpty();
            assertThat(store.getActiveFilesWithNoJobId()).isEmpty();
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME))
                    .containsExactly("file");
            assertThat(store.getPartitionToActiveFilesMap()).isEmpty();
            assertThat(store.hasNoFiles()).isFalse();
        }
    }

    @Nested
    @DisplayName("Find files for garbage collection")
    class FindFilesForGarbageCollection {

        @Test
        public void shouldFindFileWhichWasMarkedReadyForGCLongEnoughAgo() throws Exception {
            // Given
            Instant updateTime = Instant.parse("2023-10-04T14:08:00Z");
            Instant latestTimeForGc = Instant.parse("2023-10-04T14:09:00Z");
            FileReference file = factory.rootFile("readyForGc", 100L);
            store.fixTime(updateTime);
            store.addFile(file);

            // When
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("root", List.of("readyForGc"), List.of());

            // Then
            assertThat(store.getReadyForGCFilenamesBefore(latestTimeForGc))
                    .containsExactly("readyForGc");
        }

        @Test
        public void shouldNotFindFileWhichWasMarkedReadyForGCTooRecently() throws Exception {
            // Given
            Instant updateTime = Instant.parse("2023-10-04T14:08:00Z");
            Instant latestTimeForGc = Instant.parse("2023-10-04T14:07:00Z");
            FileReference file = factory.rootFile("readyForGc", 100L);
            store.fixTime(updateTime);
            store.addFile(file);

            // When
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("root", List.of("readyForGc"), List.of());

            // Then
            assertThat(store.getReadyForGCFilenamesBefore(latestTimeForGc))
                    .isEmpty();
        }

        @Test
        public void shouldNotFindFileWhichHasTwoReferencesAndOnlyOneWasMarkedAsReadyForGC() throws Exception {
            // Given
            Instant updateTime = Instant.parse("2023-10-04T14:08:00Z");
            Instant latestTimeForGc = Instant.parse("2023-10-04T14:09:00Z");
            splitPartition("root", "L", "R", 5);
            FileReference rootFile = factory.rootFile("readyForGc", 100L);
            FileReference leftFile = splitFile(rootFile, "L");
            FileReference rightFile = splitFile(rootFile, "R");
            store.fixTime(updateTime);
            store.addFiles(List.of(leftFile, rightFile));

            // When
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("L", List.of("readyForGc"), List.of());

            // Then
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
            store.fixTime(updateTime);
            store.addFiles(List.of(leftFile, rightFile));

            // When
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("L", List.of("readyForGc"), List.of());
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("R", List.of("readyForGc"), List.of());

            // Then
            assertThat(store.getReadyForGCFilenamesBefore(latestTimeForGc))
                    .containsExactly("readyForGc");
        }

        @Test
        public void shouldNotFindSplitFileWhenOnlyFirstReadyForGCUpdateIsOldEnough() throws Exception {
            // Given
            Instant addTime = Instant.parse("2023-10-04T14:08:00Z");
            Instant readyForGc1Time = Instant.parse("2023-10-04T14:09:00Z");
            Instant readyForGc2Time = Instant.parse("2023-10-04T14:10:00Z");
            Instant latestTimeForGc = Instant.parse("2023-10-04T14:09:30Z");
            splitPartition("root", "L", "R", 5);
            FileReference rootFile = factory.rootFile("readyForGc", 100L);
            FileReference leftFile = splitFile(rootFile, "L");
            FileReference rightFile = splitFile(rootFile, "R");
            store.fixTime(addTime);
            store.addFiles(List.of(leftFile, rightFile));

            // When
            store.fixTime(readyForGc1Time);
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("L", List.of("readyForGc"), List.of());
            store.fixTime(readyForGc2Time);
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("R", List.of("readyForGc"), List.of());

            // Then
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
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("root", List.of("oldFile"), List.of(newFile));

            // When
            store.deleteReadyForGCFiles(List.of("oldFile"));

            // Then
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
        }

        @Test
        void shouldDeleteGarbageCollectedFileSplitAcrossTwoPartitions() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference rootFile = factory.rootFile("file", 100L);
            FileReference leftFile = splitFile(rootFile, "L");
            FileReference rightFile = splitFile(rootFile, "R");
            store.addFiles(List.of(leftFile, rightFile));

            // When
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("L", List.of("file"), List.of());
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("R", List.of("file"), List.of());
            store.deleteReadyForGCFiles(List.of("file"));

            // Then
            assertThat(store.getActiveFiles()).isEmpty();
            assertThat(store.getActiveFilesWithNoJobId()).isEmpty();
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
            assertThat(store.getPartitionToActiveFilesMap()).isEmpty();
            assertThat(store.hasNoFiles()).isTrue();
        }

        @Test
        public void shouldFailToDeleteActiveFile() throws Exception {
            // Given
            FileReference file = factory.rootFile("test", 100L);
            store.addFile(file);

            // When / Then
            assertThatThrownBy(() -> store.deleteReadyForGCFiles(List.of("test")))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldFailToDeleteFileWhichWasNotAdded() {
            // When / Then
            assertThatThrownBy(() -> store.deleteReadyForGCFiles(List.of("test")))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldFailToDeleteActiveFileWhenOneOfTwoSplitRecordsIsReadyForGC() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference rootFile = factory.rootFile("file", 100L);
            FileReference leftFile = splitFile(rootFile, "L");
            FileReference rightFile = splitFile(rootFile, "R");
            store.addFiles(List.of(leftFile, rightFile));
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("L", List.of("file"), List.of());

            // When / Then
            assertThatThrownBy(() -> store.deleteReadyForGCFiles(List.of("file")))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldDeleteGarbageCollectedFileWhileIteratingThroughReadyForGCFiles() throws Exception {
            // Given
            FileReference oldFile1 = factory.rootFile("oldFile1", 100L);
            FileReference oldFile2 = factory.rootFile("oldFile2", 100L);
            FileReference newFile = factory.rootFile("newFile", 100L);
            store.addFiles(List.of(oldFile1, oldFile2));
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(
                    "root", List.of("oldFile1", "oldFile2"), List.of(newFile));

            // When
            Iterator<String> iterator = store.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)).iterator();
            store.deleteReadyForGCFiles(List.of(iterator.next()));

            // Then
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME))
                    .containsExactly(iterator.next());
            assertThat(iterator).isExhausted();
        }

        @Test
        public void shouldFailToDeleteActiveFileWhenAlsoDeletingReadyForGCFile() throws Exception {
            // Given
            FileReference gcFile = factory.rootFile("gcFile", 100L);
            FileReference activeFile = factory.rootFile("activeFile", 100L);
            store.addFiles(List.of(gcFile, activeFile));
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(
                    "root", List.of("gcFile"), List.of());

            // When / Then
            assertThatThrownBy(() -> store.deleteReadyForGCFiles(List.of("gcFile", "activeFile")))
                    .isInstanceOf(StateStoreException.class);
            assertThat(store.getActiveFiles())
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
            AllFileReferences report = store.getAllFileReferencesWithMaxUnreferenced(5);

            // Then
            assertThat(report).isEqualTo(activeFilesReport(file));
        }

        @Test
        void shouldReportOneReadyForGCFile() throws Exception {
            // Given
            FileReference file = factory.rootFile("test", 100L);
            store.addFile(file);
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("root", List.of("test"), List.of());

            // When
            AllFileReferences report = store.getAllFileReferencesWithMaxUnreferenced(5);

            // Then
            assertThat(report).isEqualTo(readyForGCFilesReport("test"));
        }

        @Test
        void shouldReportTwoActiveFiles() throws Exception {
            // Given
            FileReference file1 = factory.rootFile("file1", 100L);
            FileReference file2 = factory.rootFile("file2", 100L);
            store.addFiles(List.of(file1, file2));

            // When
            AllFileReferences report = store.getAllFileReferencesWithMaxUnreferenced(5);

            // Then
            assertThat(report).isEqualTo(activeFilesReport(file1, file2));
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
            AllFileReferences report = store.getAllFileReferencesWithMaxUnreferenced(5);

            // Then
            assertThat(report).isEqualTo(activeFilesReport(leftFile, rightFile));
        }

        @Test
        void shouldReportFileSplitOverTwoPartitionsWithOneReadyForGC() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference rootFile = factory.rootFile("file", 100L);
            FileReference leftFile = splitFile(rootFile, "L");
            FileReference rightFile = splitFile(rootFile, "R");
            store.addFiles(List.of(leftFile, rightFile));
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("L", List.of("file"), List.of());

            // When
            AllFileReferences report = store.getAllFileReferencesWithMaxUnreferenced(5);

            // Then
            assertThat(report).isEqualTo(activeFilesReport(rightFile));
        }

        @Test
        void shouldReportReadyForGCFilesWithLimit() throws Exception {
            // Given
            FileReference file1 = factory.rootFile("test1", 100L);
            FileReference file2 = factory.rootFile("test2", 100L);
            FileReference file3 = factory.rootFile("test3", 100L);
            store.addFiles(List.of(file1, file2, file3));
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("root", List.of("test1", "test2", "test3"), List.of());

            // When
            AllFileReferences report = store.getAllFileReferencesWithMaxUnreferenced(2);

            // Then
            assertThat(report).isEqualTo(partialReadyForGCFilesReport("test1", "test2"));
        }

        @Test
        void shouldReportReadyForGCFilesMeetingLimit() throws Exception {
            // Given
            FileReference file1 = factory.rootFile("test1", 100L);
            FileReference file2 = factory.rootFile("test2", 100L);
            store.addFiles(List.of(file1, file2));
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles("root", List.of("test1", "test2"), List.of());

            // When
            AllFileReferences report = store.getAllFileReferencesWithMaxUnreferenced(2);

            // Then
            assertThat(report).isEqualTo(readyForGCFilesReport("test1", "test2"));
        }
    }

    private void splitPartition(String parentId, String leftId, String rightId, long splitPoint) {
        partitions.splitToNewChildren(parentId, leftId, rightId, splitPoint);
        factory = FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
    }

    private FileReference splitFile(FileReference parentFile, String childPartitionId) {
        return SplitFileReference.referenceForChildPartition(parentFile, childPartitionId)
                .toBuilder().lastStateStoreUpdateTime(DEFAULT_UPDATE_TIME).build();
    }

    private FileReference splitFileByCopy(FileReference parentFile, String childPartitionId, String newFilename) {
        return SplitFileReference.copyToChildPartition(parentFile, childPartitionId, newFilename)
                .toBuilder().lastStateStoreUpdateTime(DEFAULT_UPDATE_TIME).build();
    }

    private static FileReference withLastUpdate(Instant updateTime, FileReference file) {
        return file.toBuilder().lastStateStoreUpdateTime(updateTime).build();
    }
}
