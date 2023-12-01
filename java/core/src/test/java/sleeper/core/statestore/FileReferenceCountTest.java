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

package sleeper.core.statestore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FileReferenceCountTest {
    @Test
    void shouldDecrementReferenceCount() {
        // Given
        FileReferenceCount fileReferenceCount = fileWithReferenceCount(1L);

        // When/Then
        assertThat(fileReferenceCount.decrement())
                .isEqualTo(fileWithReferenceCount(0L));
    }

    @Test
    void shouldFailToDecrementReferenceCountIfReferenceCountIsZero() {
        // Given
        FileReferenceCount fileReferenceCount = fileWithReferenceCount(0L);

        // When/Then
        assertThatThrownBy(fileReferenceCount::decrement)
                .hasMessage("File has no references")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldIncrementReferenceCount() {
        // Given
        FileReferenceCount fileReferenceCount = fileWithReferenceCount(1L);

        // When/Then
        assertThat(fileReferenceCount.increment())
                .isEqualTo(fileWithReferenceCount(2L));
    }

    private static FileReferenceCount fileWithReferenceCount(long numberOfReferences) {
        return FileReferenceCount.builder()
                .lastUpdateTime(1_000_000L)
                .filename("test-file.parquet")
                .numberOfReferences(numberOfReferences)
                .build();
    }
}
