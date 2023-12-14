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
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.SplitFileInfo;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.FilesReportTestHelper.readyForGCFileReport;
import static sleeper.core.statestore.FilesReportTestHelper.splitFileReport;
import static sleeper.core.statestore.FilesReportTestHelper.wholeFilesReport;

public class S3FileInfoStoreIT extends S3StateStoreTestBase {

    private static final Instant DEFAULT_UPDATE_TIME = Instant.parse("2023-10-04T14:08:00Z");
    private static final Instant AFTER_DEFAULT_UPDATE_TIME = DEFAULT_UPDATE_TIME.plus(Duration.ofMinutes(1));
    private final Schema schema = schemaWithKey("key", new LongType());
    private final PartitionsBuilder partitions = new PartitionsBuilder(schema).singlePartition("root");
    private FileInfoFactory factory = FileInfoFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
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
            FileInfo file1 = factory.rootFile("file1", 100L);
            FileInfo file2 = factory.rootFile("file2", 100L);
            FileInfo file3 = factory.rootFile("file3", 100L);

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
            FileInfo file = factory.rootFile("file1", 100L);

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
            FileInfo file = factory.rootFile("file1", 100L);
            store.fixTime(updateTime);
            store.addFile(file);

            // When / Then
            assertThatThrownBy(() -> store.addFile(file))
                    .isInstanceOf(StateStoreException.class);
            assertThat(store.getActiveFiles()).containsExactlyInAnyOrder(withLastUpdate(updateTime, file));
        }
    }

    @Nested
    @DisplayName("Create compaction jobs")
    class CreateCompactionJobs {

        @Test
        public void shouldMarkFileWithJobId() throws Exception {
            // Given
            FileInfo file = factory.rootFile("file", 100L);
            store.addFile(file);

            // When
            store.atomicallyUpdateJobStatusOfFiles("job", Collections.singletonList(file));

            // Then
            assertThat(store.getActiveFiles()).containsExactly(file.toBuilder().jobId("job").build());
            assertThat(store.getActiveFilesWithNoJobId()).isEmpty();
        }

        @Test
        public void shouldNotMarkFileWithJobIdWhenOneIsAlreadySet() throws Exception {
            // Given
            FileInfo file = factory.rootFile("file", 100L);
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
            FileInfo file1 = factory.rootFile("file1", 100L);
            FileInfo file2 = factory.rootFile("file2", 100L);
            FileInfo file3 = factory.rootFile("file3", 100L);
            store.addFiles(Arrays.asList(file1, file2, file3));
            store.atomicallyUpdateJobStatusOfFiles("job1", Collections.singletonList(file2));

            // When / Then
            assertThatThrownBy(() -> store.atomicallyUpdateJobStatusOfFiles("job2", Arrays.asList(file1, file2, file3)))
                    .isInstanceOf(StateStoreException.class);
            assertThat(store.getActiveFiles()).containsExactlyInAnyOrder(
                    file1, file2.toBuilder().jobId("job1").build(), file3);
            assertThat(store.getActiveFilesWithNoJobId()).containsExactlyInAnyOrder(file1, file3);
        }
    }

    @Nested
    @DisplayName("Apply compaction")
    class ApplyCompaction {

        @Test
        public void shouldSetFileReadyForGC() throws Exception {
            // Given
            FileInfo oldFile = factory.rootFile("oldFile", 100L);
            FileInfo newFile = factory.rootFile("newFile", 100L);
            store.addFile(oldFile);

            // When
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(Collections.singletonList(oldFile), newFile);
            store.fixTime(DEFAULT_UPDATE_TIME.plus(Duration.ofMinutes(10)));

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
        void shouldSplitFileAcrossTwoPartitions() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileInfo rootFile = factory.rootFile("file", 100L);
            FileInfo leftFile = splitFile(rootFile, "L");
            FileInfo rightFile = splitFile(rootFile, "R");
            store.addFile(rootFile);

            // When
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List.of(rootFile), List.of(leftFile, rightFile));
            store.fixTime(DEFAULT_UPDATE_TIME.plus(Duration.ofMinutes(10)));

            // Then
            assertThat(store.getActiveFiles()).containsExactlyInAnyOrder(leftFile, rightFile);
            assertThat(store.getActiveFilesWithNoJobId()).containsExactlyInAnyOrder(leftFile, rightFile);
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
            assertThat(store.getPartitionToActiveFilesMap())
                    .isEqualTo(Map.of("L", List.of("file"), "R", List.of("file")));
        }

        @Test
        void shouldFailToSetReadyForGCWhenAlreadyReadyForGC() throws Exception {
            // Given
            FileInfo oldFile = factory.rootFile("oldFile", 100L);
            FileInfo newFile = factory.rootFile("newFile", 100L);
            store.addFile(oldFile);

            // When
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(List.of(oldFile), newFile);
            store.fixTime(DEFAULT_UPDATE_TIME.plus(Duration.ofMinutes(10)));

            // Then
            assertThatThrownBy(() -> store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(List.of(oldFile), newFile))
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
    }

    @Nested
    @DisplayName("Find files for garbage collection")
    class FindFilesForGarbageCollection {

        @Test
        public void shouldFindFileWhichWasMarkedReadyForGCLongEnoughAgo() throws Exception {
            // Given
            Instant updateTime = Instant.parse("2023-10-04T14:08:00Z");
            Instant latestTimeForGc = Instant.parse("2023-10-04T14:09:00Z");
            FileInfo file = factory.rootFile("readyForGc", 100L);
            store.fixTime(updateTime);
            store.addFile(file);

            // When
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List.of(file), List.of());

            // Then
            assertThat(store.getReadyForGCFilenamesBefore(latestTimeForGc))
                    .containsExactly("readyForGc");
        }

        @Test
        public void shouldNotFindFileWhichWasMarkedReadyForGCTooRecently() throws Exception {
            // Given
            Instant updateTime = Instant.parse("2023-10-04T14:08:00Z");
            Instant latestTimeForGc = Instant.parse("2023-10-04T14:07:00Z");
            FileInfo file = factory.rootFile("readyForGc", 100L);
            store.fixTime(updateTime);
            store.addFile(file);

            // When
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List.of(file), List.of());

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
            FileInfo rootFile = factory.rootFile("readyForGc", 100L);
            FileInfo leftFile = splitFile(rootFile, "L");
            FileInfo rightFile = splitFile(rootFile, "R");
            store.fixTime(updateTime);
            store.addFiles(List.of(leftFile, rightFile));

            // When
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List.of(leftFile), List.of());

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
            FileInfo rootFile = factory.rootFile("readyForGc", 100L);
            FileInfo leftFile = splitFile(rootFile, "L");
            FileInfo rightFile = splitFile(rootFile, "R");
            store.fixTime(updateTime);
            store.addFiles(List.of(leftFile, rightFile));

            // When
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List.of(leftFile), List.of());
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List.of(rightFile), List.of());

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
            FileInfo rootFile = factory.rootFile("readyForGc", 100L);
            FileInfo leftFile = splitFile(rootFile, "L");
            FileInfo rightFile = splitFile(rootFile, "R");
            store.fixTime(addTime);
            store.addFiles(List.of(leftFile, rightFile));

            // When
            store.fixTime(readyForGc1Time);
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List.of(leftFile), List.of());
            store.fixTime(readyForGc2Time);
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List.of(rightFile), List.of());

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
            FileInfo oldFile = factory.rootFile("oldFile", 100L);
            FileInfo newFile = factory.rootFile("newFile", 100L);
            store.addFile(oldFile);
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(Collections.singletonList(oldFile), newFile);

            // When
            store.deleteReadyForGCFile("oldFile");

            // Then
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
        }

        @Test
        void shouldDeleteGarbageCollectedFileSplitAcrossTwoPartitions() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileInfo rootFile = factory.rootFile("file", 100L);
            FileInfo leftFile = splitFile(rootFile, "L");
            FileInfo rightFile = splitFile(rootFile, "R");
            store.addFiles(List.of(leftFile, rightFile));

            // When
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List.of(leftFile), List.of());
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List.of(rightFile), List.of());
            store.deleteReadyForGCFile("file");

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
            FileInfo file = factory.rootFile("test", 100L);
            store.addFile(file);

            // When / Then
            assertThatThrownBy(() -> store.deleteReadyForGCFile("test"))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldFailToDeleteFileWhichWasNotAdded() {
            // When / Then
            assertThatThrownBy(() -> store.deleteReadyForGCFile("test"))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldFailToDeleteActiveFileWhenOneOfTwoSplitRecordsIsReadyForGC() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileInfo rootFile = factory.rootFile("file", 100L);
            FileInfo leftFile = splitFile(rootFile, "L");
            FileInfo rightFile = splitFile(rootFile, "R");
            store.addFiles(List.of(leftFile, rightFile));
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List.of(leftFile), List.of());

            // When / Then
            assertThatThrownBy(() -> store.deleteReadyForGCFile("file"))
                    .isInstanceOf(StateStoreException.class);
        }
    }

    @Nested
    @DisplayName("Report file status")
    class ReportFileStatus {

        @Test
        void shouldReportOneActiveFile() throws Exception {
            // Given
            FileInfo file = factory.rootFile("test", 100L);
            store.addFile(file);

            // When
            AllFileReferences report = store.getAllFileReferences();

            // Then
            assertThat(report).isEqualTo(wholeFilesReport(file));
        }

        @Test
        void shouldReportOneReadyForGCFile() throws Exception {
            // Given
            FileInfo file = factory.rootFile("test", 100L);
            store.addFile(file);
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List.of(file), List.of());

            // When
            AllFileReferences report = store.getAllFileReferences();

            // Then
            assertThat(report).isEqualTo(readyForGCFileReport("test", DEFAULT_UPDATE_TIME));
        }

        @Test
        void shouldReportTwoActiveFiles() throws Exception {
            // Given
            FileInfo file1 = factory.rootFile("file1", 100L);
            FileInfo file2 = factory.rootFile("file2", 100L);
            store.addFiles(List.of(file1, file2));

            // When
            AllFileReferences report = store.getAllFileReferences();

            // Then
            assertThat(report).isEqualTo(wholeFilesReport(file1, file2));
        }

        @Test
        void shouldReportFileSplitOverTwoPartitions() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileInfo rootFile = factory.rootFile("file", 100L);
            FileInfo leftFile = splitFile(rootFile, "L");
            FileInfo rightFile = splitFile(rootFile, "R");
            store.addFiles(List.of(leftFile, rightFile));

            // When
            AllFileReferences report = store.getAllFileReferences();

            // Then
            assertThat(report).isEqualTo(splitFileReport("file", DEFAULT_UPDATE_TIME, leftFile, rightFile));
        }

        @Test
        void shouldReportFileSplitOverTwoPartitionsWithOneReadyForGC() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileInfo rootFile = factory.rootFile("file", 100L);
            FileInfo leftFile = splitFile(rootFile, "L");
            FileInfo rightFile = splitFile(rootFile, "R");
            store.addFiles(List.of(leftFile, rightFile));
            store.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List.of(leftFile), List.of());

            // When
            AllFileReferences report = store.getAllFileReferences();

            // Then
            assertThat(report).isEqualTo(wholeFilesReport(rightFile));
        }
    }

    private void splitPartition(String parentId, String leftId, String rightId, long splitPoint) {
        partitions.splitToNewChildren(parentId, leftId, rightId, splitPoint);
        factory = FileInfoFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
    }

    private FileInfo splitFile(FileInfo parentFile, String childPartitionId) {
        return SplitFileInfo.referenceForChildPartition(parentFile, childPartitionId)
                .toBuilder().lastStateStoreUpdateTime(DEFAULT_UPDATE_TIME).build();
    }

    private static FileInfo withLastUpdate(Instant updateTime, FileInfo file) {
        return file.toBuilder().lastStateStoreUpdateTime(updateTime).build();
    }
}
