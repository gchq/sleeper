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
package sleeper.statestore.dynamodb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.AllReferencesToAFileTestHelper;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.SplitFileReference;
import sleeper.core.statestore.SplitFileReferenceRequest;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceAssignedToJobException;
import sleeper.core.statestore.exception.SplitRequestsFailedException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.core.statestore.FileReferenceTestData.splitFile;
import static sleeper.core.statestore.FileReferenceTestData.withJobId;
import static sleeper.core.statestore.FileReferenceTestData.withLastUpdate;
import static sleeper.core.statestore.FilesReportTestHelper.activeFilesReport;
import static sleeper.core.statestore.FilesReportTestHelper.noFilesReport;
import static sleeper.core.statestore.SplitFileReferenceRequest.splitFileToChildPartitions;

public class DynamoDBFileReferenceStoreDynamoSpecificIT extends DynamoDBStateStoreOneTableTestBase {

    @BeforeEach
    void setUp() throws Exception {
        initialiseWithSchema(schemaWithKey("key", new LongType()));
    }

    @Nested
    @DisplayName("Add and delete many files")
    class ManyFiles {

        @Test
        public void shouldAddAndRetrieve1000FileReferences() throws Exception {
            // Given
            List<FileReference> files = IntStream.range(0, 1000)
                    .mapToObj(i -> factory.rootFile("file-" + i, 1))
                    .collect(Collectors.toUnmodifiableList());
            store.fixFileUpdateTime(Instant.ofEpochMilli(1_000_000L));

            // When
            store.addFiles(files);

            // Then
            assertThat(new HashSet<>(store.getFileReferences())).isEqualTo(files.stream()
                    .map(reference -> withLastUpdate(Instant.ofEpochMilli(1_000_000L), reference))
                    .collect(Collectors.toSet()));
        }

        @Test
        public void shouldDeleteMoreThan100ReadyForGCFiles() throws Exception {
            // Given
            Instant updateTime = Instant.parse("2023-10-04T14:08:00Z");
            Instant afterUpdateTime = updateTime.plus(Duration.ofMinutes(2));

            List<String> filenames = IntStream.range(0, 101)
                    .mapToObj(i -> "gcFile" + i)
                    .collect(toUnmodifiableList());

            store.fixFileUpdateTime(updateTime);
            store.addFilesWithReferences(filenames.stream()
                    .map(AllReferencesToAFileTestHelper::fileWithNoReferences)
                    .collect(toUnmodifiableList()));

            assertThat(store.getReadyForGCFilenamesBefore(afterUpdateTime))
                    .hasSize(101);

            // When / Then
            store.deleteGarbageCollectedFileReferenceCounts(filenames);
            assertThat(store.getFileReferences())
                    .isEmpty();
            assertThat(store.getReadyForGCFilenamesBefore(afterUpdateTime))
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("Split file references transactionally")
    class SplitReferenceTransactions {

        @Test
        void shouldSucceedIfThereAreOver25SplitRequests() throws Exception {
            // Given
            List<FileReference> fileReferences = new ArrayList<>();
            List<SplitFileReferenceRequest> splitRequests = new ArrayList<>();
            for (int i = 1; i <= 26; i++) {
                FileReference file = factory.rootFile("file" + i, 100L);
                fileReferences.add(file);
                splitRequests.add(splitFileToChildPartitions(file, "L", "R"));
            }
            store.addFiles(fileReferences);

            // When
            store.splitFileReferences(splitRequests);

            // Then
            List<FileReference> expectedReferences = fileReferences.stream()
                    .flatMap(file -> Stream.of(splitFile(file, "L"), splitFile(file, "R")))
                    .collect(toUnmodifiableList());
            assertThat(store.getFileReferences())
                    .containsExactlyInAnyOrderElementsOf(expectedReferences);
            assertThat(store.getAllFilesWithMaxUnreferenced(100))
                    .isEqualTo(activeFilesReport(DEFAULT_UPDATE_TIME, expectedReferences));
        }

        @Test
        void shouldThrowExceptionIfThereAre26SplitRequestsAndTheLastOneFails() throws Exception {
            // Given
            List<FileReference> fileReferences = new ArrayList<>();
            List<SplitFileReferenceRequest> splitRequests = new ArrayList<>();
            for (int i = 1; i <= 25; i++) {
                FileReference file = factory.rootFile("file" + i, 100L);
                fileReferences.add(file);
                splitRequests.add(splitFileToChildPartitions(file, "L", "R"));
            }
            splitRequests.add(splitFileToChildPartitions(factory.rootFile("not-found", 100L), "L", "R"));
            store.addFiles(fileReferences);

            // When / Then
            assertThatThrownBy(() -> store.splitFileReferences(splitRequests))
                    .isInstanceOfSatisfying(SplitRequestsFailedException.class, exception -> assertThat(exception)
                            .extracting(SplitRequestsFailedException::getSuccessfulRequests,
                                    SplitRequestsFailedException::getFailedRequests)
                            .containsExactly(splitRequests.subList(0, 25), splitRequests.subList(25, 26)))
                    .hasCauseInstanceOf(FileNotFoundException.class);
            List<FileReference> expectedReferences = fileReferences.stream()
                    .flatMap(file -> Stream.of(splitFile(file, "L"), splitFile(file, "R")))
                    .collect(toUnmodifiableList());
            assertThat(store.getFileReferences())
                    .containsExactlyInAnyOrderElementsOf(expectedReferences);
            assertThat(store.getAllFilesWithMaxUnreferenced(100))
                    .isEqualTo(activeFilesReport(DEFAULT_UPDATE_TIME, expectedReferences));
        }

        @Test
        void shouldThrowExceptionIfTheFirstRequestFails() throws Exception {
            // Given
            FileReference file = factory.rootFile("file", 100L);

            // When / Then
            SplitFileReferenceRequest request = splitFileToChildPartitions(file, "L", "R");
            assertThatThrownBy(() -> store.splitFileReferences(List.of(request)))
                    .isInstanceOfSatisfying(SplitRequestsFailedException.class, exception -> assertThat(exception)
                            .extracting(SplitRequestsFailedException::getSuccessfulRequests,
                                    SplitRequestsFailedException::getFailedRequests)
                            .containsExactly(List.of(), List.of(request)))
                    .hasCauseInstanceOf(FileNotFoundException.class);
            assertThat(store.getFileReferences()).isEmpty();
            assertThat(store.getAllFilesWithMaxUnreferenced(100))
                    .isEqualTo(noFilesReport());
        }

        @Test
        void shouldThrowExceptionIfOneRequestDoesNotFitInATransaction() throws Exception {
            // Given
            FileReference file = factory.rootFile("file", 100L);

            // When / Then
            SplitFileReferenceRequest request = new SplitFileReferenceRequest(file, IntStream.range(0, 100)
                    .mapToObj(i -> SplitFileReference.referenceForChildPartition(file, "" + i, 1))
                    .collect(toUnmodifiableList()));
            assertThatThrownBy(() -> store.splitFileReferences(List.of(request)))
                    .isInstanceOfSatisfying(SplitRequestsFailedException.class, exception -> assertThat(exception)
                            .extracting(SplitRequestsFailedException::getSuccessfulRequests,
                                    SplitRequestsFailedException::getFailedRequests)
                            .containsExactly(List.of(), List.of(request)))
                    .hasNoCause();
            assertThat(store.getFileReferences()).isEmpty();
            assertThat(store.getAllFilesWithMaxUnreferenced(100))
                    .isEqualTo(noFilesReport());
        }
    }

    @Nested
    @DisplayName("Batch job assignment")
    class BatchJobAssignment {

        @Test
        public void shouldFailWhenSameFileIsAssignedInDifferentRequests() throws Exception {
            // Given
            FileReference file1 = factory.rootFile("file1", 100L);
            FileReference file2 = factory.rootFile("file2", 100L);
            store.addFiles(List.of(file1, file2));

            // When / Then
            assertThatThrownBy(() -> store.assignJobIds(List.of(
                    assignJobOnPartitionToFiles("job1", "root", List.of("file1")),
                    assignJobOnPartitionToFiles("job2", "root", List.of("file1")))))
                    .isInstanceOf(FileReferenceAssignedToJobException.class);
            assertThat(store.getFileReferences()).containsExactly(
                    withJobId("job1", file1),
                    file2);
            assertThat(store.getFileReferencesWithNoJobId()).containsExactly(file2);
        }

    }
}
