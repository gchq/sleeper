/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.garbagecollector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.table.TableStatus;
import sleeper.garbagecollector.FailedGarbageCollectionException.FileFailure;
import sleeper.garbagecollector.FailedGarbageCollectionException.TableFailures;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class FileDeletionTrackerTest {

    FilesToDeleteInBucket files = new FilesToDeleteInBucket("test-bucket", Map.of(
            "file-1.parquet", "file-1",
            "file-1.sketches", "file-1",
            "file-2.parquet", "file-2",
            "file-2.sketches", "file-2"));
    FileDeletionTracker tracker = new FileDeletionTracker(files);
    TableFilesDeleted deleted = new TableFilesDeleted(TableStatus.uniqueIdAndName("test-table-id", "test-table", true));

    @BeforeEach
    void setUp() {
        deleted.startBatch(List.of("file-1", "file-2"));
    }

    @Test
    void shouldRecordFileAsDeletedWhenAllObjectsDeleted() {
        // When
        tracker.objectDeleted("file-1.parquet");
        tracker.objectDeleted("file-1.sketches");
        tracker.recordFileResults(deleted);

        // Then
        assertThat(deleted.getFilesDeletedInBatch()).containsExactly("file-1");
        assertThat(fileFailures()).isEmpty();
    }

    @Test
    void shouldNotRecordFileAsDeletedWhenOnlySomeObjectsDeleted() {
        // When
        tracker.objectDeleted("file-1.parquet");
        tracker.recordFileResults(deleted);

        // Then
        assertThat(deleted.getFilesDeletedInBatch()).isEmpty();
        assertThat(fileFailures()).isEmpty();
    }

    @Test
    void shouldRecordFileAsFailedWhenOneObjectFailed() {
        // Given
        Exception failure = new RuntimeException("Failed sketches");

        // When
        tracker.objectDeleted("file-1.parquet");
        tracker.objectFailed("file-1.sketches", failure);
        tracker.recordFileResults(deleted);

        // Then
        assertThat(deleted.getFilesDeletedInBatch()).isEmpty();
        assertThat(fileFailures()).containsExactly(new FileFailure(List.of("file-1"), failure));
    }

    @Test
    void shouldRecordFileAsFailedWhenObjectFailedBeforeOtherObjectDeleted() {
        // Given
        Exception failure = new RuntimeException("Failed data file");

        // When
        tracker.objectFailed("file-1.parquet", failure);
        tracker.objectDeleted("file-1.sketches");
        tracker.recordFileResults(deleted);

        // Then
        assertThat(deleted.getFilesDeletedInBatch()).isEmpty();
        assertThat(fileFailures()).containsExactly(new FileFailure(List.of("file-1"), failure));
    }

    @Test
    void shouldRecordOtherFileAsDeletedWhenOneFileFailed() {
        // Given
        Exception failure = new RuntimeException("Failed sketches");

        // When
        tracker.objectDeleted("file-1.parquet");
        tracker.objectFailed("file-1.sketches", failure);
        tracker.objectDeleted("file-2.parquet");
        tracker.objectDeleted("file-2.sketches");
        tracker.recordFileResults(deleted);

        // Then
        assertThat(deleted.getFilesDeletedInBatch()).containsExactly("file-2");
        assertThat(fileFailures()).containsExactly(new FileFailure(List.of("file-1"), failure));
    }

    @Test
    void shouldRecordOneFailureForAllFilesInFailedRequest() {
        // Given
        Exception failure = new RuntimeException("Failed request");

        // When
        tracker.objectsFailed(List.of("file-1.parquet", "file-1.sketches", "file-2.parquet", "file-2.sketches"), failure);
        tracker.recordFileResults(deleted);

        // Then
        assertThat(deleted.getFilesDeletedInBatch()).isEmpty();
        assertThat(fileFailures()).containsExactly(new FileFailure(List.of("file-1", "file-2"), failure));
    }

    @Test
    void shouldRecordSeparateFailuresForDifferentErrors() {
        // Given
        Exception failure1 = new RuntimeException("Failed file 1");
        Exception failure2 = new RuntimeException("Failed file 2");

        // When
        tracker.objectFailed("file-1.parquet", failure1);
        tracker.objectFailed("file-2.sketches", failure2);
        tracker.recordFileResults(deleted);

        // Then
        assertThat(fileFailures()).containsExactly(
                new FileFailure(List.of("file-1"), failure1),
                new FileFailure(List.of("file-2"), failure2));
    }

    @Test
    void shouldKeepFirstFailureWhenBothObjectsForFileFailed() {
        // Given
        Exception failure1 = new RuntimeException("Failed data file");
        Exception failure2 = new RuntimeException("Failed sketches");

        // When
        tracker.objectFailed("file-1.parquet", failure1);
        tracker.objectFailed("file-1.sketches", failure2);
        tracker.recordFileResults(deleted);

        // Then
        assertThat(fileFailures()).containsExactly(new FileFailure(List.of("file-1"), failure1));
    }

    private List<FileFailure> fileFailures() {
        return deleted.buildTableFailures()
                .map(TableFailures::fileFailures)
                .orElse(List.of());
    }
}
