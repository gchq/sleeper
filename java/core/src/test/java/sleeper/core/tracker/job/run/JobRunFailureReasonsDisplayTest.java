/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.core.tracker.job.run;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class JobRunFailureReasonsDisplayTest {

    @Test
    void shouldShowOneFailureReason() {
        // Given
        List<String> reasons = List.of("Some reason");

        // When / Then
        assertThat(JobRunFailureReasons.getFailureReasonsDisplay(30, reasons))
                .isEqualTo("Some reason.");
    }

    @Test
    void shouldShowTwoFailureReasons() {
        // Given
        List<String> reasons = List.of("Some reason", "Other reason");

        // When / Then
        assertThat(JobRunFailureReasons.getFailureReasonsDisplay(50, reasons))
                .isEqualTo("Some reason. Other reason.");
    }

    @Test
    void shouldTruncateOneReason() {
        // Given
        List<String> reasons = List.of("1234567890");

        // When / Then
        assertThat(JobRunFailureReasons.getFailureReasonsDisplay(8, reasons))
                .isEqualTo("12345...");
    }

    @Test
    void shouldTruncateTwoReasons() {
        // Given
        List<String> reasons = List.of("12345", "67890");

        // When / Then
        assertThat(JobRunFailureReasons.getFailureReasonsDisplay(11, reasons))
                .isEqualTo("12345. 6...");
    }

    @Test
    void shouldTruncateBetweenTwoReasonsAtEndOfFirst() {
        // Given
        List<String> reasons = List.of("12345", "67890");

        // When / Then
        assertThat(JobRunFailureReasons.getFailureReasonsDisplay(8, reasons))
                .isEqualTo("12345...");
    }

    @Test
    void shouldTruncateBetweenTwoReasonsAtStartOfSecond() {
        // Given
        List<String> reasons = List.of("12345", "67890");

        // When / Then
        assertThat(JobRunFailureReasons.getFailureReasonsDisplay(10, reasons))
                .isEqualTo("12345...");
    }

    @Test
    void shouldTruncateWithNoSpaceForEllipsis() {
        // Given
        List<String> reasons = List.of("Some reason");

        // When / Then
        assertThat(JobRunFailureReasons.getFailureReasonsDisplay(2, reasons))
                .isEqualTo("...");
    }

    @Test
    void shouldReturnNullWithNoFailureReason() {
        // Given
        List<String> reasons = List.of();

        // When / Then
        assertThat(JobRunFailureReasons.getFailureReasonsDisplay(30, reasons))
                .isNull();
    }

    @Test
    void shouldReturnNullWithNullFailureReasons() {
        // Given
        List<String> reasons = null;

        // When / Then
        assertThat(JobRunFailureReasons.getFailureReasonsDisplay(30, reasons))
                .isNull();
    }

}
