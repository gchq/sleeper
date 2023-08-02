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

package sleeper.ingest.testutils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ResultVerifierTest {
    @Nested
    @DisplayName("Calculate expected number of files")
    class CalculateExpectedNumberOfFiles {
        @Test
        void shouldCalculateNumberOfFilesWithNoRecords() {
            // Given
            int numberOfRecords = 0;
            int maxRecordsInMemory = 10;

            // When
            int numberOfFiles = ResultVerifier.calculateNumberOfFiles(numberOfRecords, maxRecordsInMemory);

            // Then
            assertThat(numberOfFiles).isEqualTo(0);
        }

        @Test
        void shouldCalculateNumberOfFilesWhenMaxRecordsInMemoryIsDivisibleByNumberOfRecords() {
            // Given
            int numberOfRecords = 5;
            int maxRecordsInMemory = 10;

            // When
            int numberOfFiles = ResultVerifier.calculateNumberOfFiles(numberOfRecords, maxRecordsInMemory);

            // Then
            assertThat(numberOfFiles).isEqualTo(2);
        }

        @Test
        void shouldCalculateNumberOfFilesWhenMaxRecordsInMemoryIsNotDivisibleByNumberOfRecords() {
            // Given
            int numberOfRecords = 5;
            int maxRecordsInMemory = 6;

            // When
            int numberOfFiles = ResultVerifier.calculateNumberOfFiles(numberOfRecords, maxRecordsInMemory);

            // Then
            assertThat(numberOfFiles).isEqualTo(2);
        }
    }
}
