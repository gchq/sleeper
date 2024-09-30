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
package sleeper.core.statestore.testutils;

import org.junit.jupiter.api.Test;

import sleeper.core.statestore.StateStoreException;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class InMemoryFileReferenceStoreExceptionTest {

    private final InMemoryFileReferenceStore fileStore = new InMemoryFileReferenceStore();

    @Test
    void shouldFailQueryWithExpectedRuntimeException() {
        // Given
        RuntimeException e = new RuntimeException("Test failure");
        fileStore.setFailuresForExpectedQueries(List.of(Optional.of(e)));

        // When / Then
        assertThatThrownBy(fileStore::getFileReferences)
                .isSameAs(e);
    }

    @Test
    void shouldFailQueryWithExpectedStateStoreException() {
        // Given
        StateStoreException e = new StateStoreException("Test failure");
        fileStore.setFailuresForExpectedQueries(List.of(Optional.of(e)));

        // When / Then
        assertThatThrownBy(fileStore::getFileReferences)
                .isSameAs(e);
    }

    @Test
    void shouldFailQueryWithUnexpectedExceptionType() {
        // Given
        Exception e = new Exception("Test failure");
        fileStore.setFailuresForExpectedQueries(List.of(Optional.of(e)));

        // When / Then
        assertThatThrownBy(fileStore::getFileReferences)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldFailReadyForGCQueryWithExpectedException() {
        // Given
        RuntimeException e = new RuntimeException("Test failure");
        fileStore.setFailuresForExpectedQueries(List.of(Optional.of(e)));

        // When / Then
        assertThatThrownBy(() -> fileStore.getReadyForGCFilenamesBefore(Instant.now()))
                .isSameAs(e);
    }

}
