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

package sleeper.systemtest.compaction;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class CompactionPerformanceCheckerTest {

    @DisplayName("Compaction job count assertions")
    @Nested
    class CheckCompactionJobCount {
        @Test
        void shouldNotThrowExceptionWhenCompactionJobCountWasExpected() {
            // Given
            CompactionPerformanceResults results = withActualNumberOfJobs(1);

            // When/Then
            assertThatCode(() -> runCheck(results))
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldThrowExceptionWhenCompactionJobCountWasNotExpected() {
            // Given
            CompactionPerformanceResults results = withActualNumberOfJobs(2);

            // When/Then
            assertThatThrownBy(() -> runCheck(results))
                    .isInstanceOf(CompactionPerformanceChecker.CheckFailedException.class)
                    .hasMessageContaining("Actual number of compaction jobs");
        }
    }

    @DisplayName("Records in root partition assertions")
    @Nested
    class CheckRecordsInRootPartition {
        @Test
        void shouldNotThrowExceptionWhenRecordsInRootPartitionIsExpected() {
            // Given
            CompactionPerformanceResults results = withActualRecordsInRoot(100);

            // When/Then
            assertThatCode(() -> runCheck(results))
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldThrowExceptionWhenRecordsInRootPartitionIsNotExpected() {
            // Given
            CompactionPerformanceResults results = withActualRecordsInRoot(101);

            // When/Then
            assertThatThrownBy(() -> runCheck(results))
                    .isInstanceOf(CompactionPerformanceChecker.CheckFailedException.class)
                    .hasMessageContaining("Actual number of records in root partition");
        }
    }

    @DisplayName("Average read/write rate assertions")
    @Nested
    class CheckAverageRates {
        @Test
        void shouldNotThrowExceptionWhenReadRateIsBetterThanPreviousRate() {
            // Given
            CompactionPerformanceResults results = withActualReadRate(0.6);

            // When/Then
            assertThatCode(() -> runCheck(results))
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldThrowExceptionWhenReadRateIsWorseThanPreviousRate() {
            // Given
            CompactionPerformanceResults results = withActualReadRate(0.4);

            // When/Then
            assertThatThrownBy(() -> runCheck(results))
                    .isInstanceOf(CompactionPerformanceChecker.CheckFailedException.class)
                    .hasMessageContaining("Read rate");
        }

        @Test
        void shouldNotThrowExceptionWhenWriteRateIsBetterThanPreviousRate() {
            // Given
            CompactionPerformanceResults results = withActualWriteRate(0.6);

            // When/Then
            assertThatCode(() -> runCheck(results))
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldThrowExceptionWhenWriteRateIsWorseThanPreviousRate() {
            // Given
            CompactionPerformanceResults results = withActualWriteRate(0.4);


            // When/Then
            assertThatThrownBy(() -> runCheck(results))
                    .isInstanceOf(CompactionPerformanceChecker.CheckFailedException.class)
                    .hasMessageContaining("Write rate");
        }
    }

    private CompactionPerformanceResults.Builder withValidDefaults() {
        return CompactionPerformanceResults.builder()
                .numOfJobs(1)
                .numOfRecordsInRoot(100)
                .readRate(0.5)
                .writeRate(0.5);
    }

    private CompactionPerformanceResults withActualNumberOfJobs(int actualNumberOfJobs) {
        return withValidDefaults()
                .numOfJobs(actualNumberOfJobs)
                .build();
    }

    private CompactionPerformanceResults withActualRecordsInRoot(int actualRecordsInRoot) {
        return withValidDefaults()
                .numOfRecordsInRoot(actualRecordsInRoot)
                .build();
    }

    private CompactionPerformanceResults withActualReadRate(double actualReadRate) {
        return withValidDefaults()
                .readRate(actualReadRate)
                .build();
    }

    private CompactionPerformanceResults withActualWriteRate(double actualWriteRate) {
        return withValidDefaults()
                .writeRate(actualWriteRate)
                .build();
    }

    private void runCheck(CompactionPerformanceResults results) throws CompactionPerformanceChecker.CheckFailedException {
        CompactionPerformanceChecker.check(results,
                1, 100, 0.5, 0.5);
    }
}
