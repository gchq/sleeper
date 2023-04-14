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

public class RunCompactionPerformanceCheckTest {
    private final CompactionPerformanceChecker checker = createCheckerInMemory();

    @DisplayName("Compaction job count assertions")
    @Nested
    class AssertCompactionJobCount {
        @Test
        void shouldNotThrowExceptionWhenCompactionJobCountWasExpected() {
            // Given
            RunCompactionPerformanceCheck runCheck = runCheckBuilder()
                    .expectedNumOfJobs(1).build();

            // When/Then
            assertThatCode(runCheck::run)
                    .doesNotThrowAnyException();
        }
    }

    private CompactionPerformanceChecker createCheckerInMemory() {
        return CompactionPerformanceCheckerInMemory.builder()
                .actualNumOfJobs(1)
                .build();
    }

    private RunCompactionPerformanceCheck.Builder runCheckBuilder() {
        return RunCompactionPerformanceCheck.builder()
                .checker(checker);
    }
}
