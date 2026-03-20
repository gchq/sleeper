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
package sleeper.clients.report.filestatus;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.withinPercentage;

public class FileReferenceStatsTest {

    @Test
    public void shouldReturnNullMean() {
        // Given
        int total = 0;
        int count = 0;

        // When
        Double mean = FileReferencesStats.mean(total, count);

        // Then
        assertThat(mean).isNull();
    }

    @Test
    public void shouldReturnValidMean() {
        // Given
        int total = 100;
        int count = 4;

        // When
        Double mean = FileReferencesStats.mean(total, count);

        // Then
        assertThat(mean).isCloseTo(25.0, withinPercentage(1));
    }

    @Test
    public void shouldReturnNullMedian() {
        // Given
        List<Integer> values = List.of();

        // When
        Double median = FileReferencesStats.median(values);

        // Then
        assertThat(median).isNull();
    }

    @Test
    public void shouldReturnValidSingleMedian() {
        // Given
        List<Integer> values = List.of(5);

        // When
        Double median = FileReferencesStats.median(values);

        // Then
        assertThat(median).isCloseTo(5d, withinPercentage(0));
    }

    @Test
    public void shouldReturnValidMultipleMedian() {
        // Given
        List<Integer> values = List.of(5, 5);

        // When
        Double median = FileReferencesStats.median(values);

        // Then
        assertThat(median).isCloseTo(5d, withinPercentage(0));
    }

    @Test
    public void shouldReturnValidMultipleMedian2() {
        // Given
        List<Integer> values = List.of(5, 5, 5);

        // When
        Double median = FileReferencesStats.median(values);

        // Then
        assertThat(median).isCloseTo(5d, withinPercentage(0));
    }

    @Test
    public void shouldReturnValidMultipleMedian3() {
        // Given
        List<Integer> values = List.of(5, 6, 7, 8);

        // When
        Double median = FileReferencesStats.median(values);

        // Then
        assertThat(median).isCloseTo(6.5d, withinPercentage(1));
    }

    @Test
    public void shouldReturnValidMultipleMedian4() {
        // Given
        List<Integer> values = List.of(5, 6, 7, 8, 9);

        // When
        Double median = FileReferencesStats.median(values);

        // Then
        assertThat(median).isCloseTo(7d, withinPercentage(0));
    }

    @Test
    public void shouldReturnNullPercentile() {
        // Given
        List<Integer> values = List.of();

        // When
        Double pc = FileReferencesStats.percentile(values, 0d);

        // Then
        assertThat(pc).isNull();
    }

    @Test
    public void shouldThrowOnInvalidPercentile() {
        // Given
        List<Integer> values = List.of();

        // When - exception
        assertThatIllegalArgumentException().isThrownBy(
                () -> FileReferencesStats.percentile(values, 1.1d))
                .withMessage("percentile must be in range [0, 1]: 1.1");
    }

    @Test
    public void shouldReturnSinglePercentile() {
        // Given
        List<Integer> values = List.of(7);

        // When
        Double pc0 = FileReferencesStats.percentile(values, 0d);
        Double pc50 = FileReferencesStats.percentile(values, 0.5d);
        Double pc99 = FileReferencesStats.percentile(values, 0.99d);
        Double pc100 = FileReferencesStats.percentile(values, 1d);

        // Then
        assertThat(pc0).isCloseTo(7, withinPercentage(0));
        assertThat(pc50).isCloseTo(7, withinPercentage(0));
        assertThat(pc99).isCloseTo(7, withinPercentage(0));
        assertThat(pc100).isCloseTo(7, withinPercentage(0));
    }

    @Test
    public void shouldReturnMultiplePercentile() {
        // Given
        List<Integer> values = List.of(7, 8);

        // When
        Double pc0 = FileReferencesStats.percentile(values, 0d);
        Double pc50 = FileReferencesStats.percentile(values, 0.5d);
        Double pc99 = FileReferencesStats.percentile(values, 0.99d);
        Double pc100 = FileReferencesStats.percentile(values, 1d);

        // Then
        assertThat(pc0).isCloseTo(7, withinPercentage(0));
        assertThat(pc50).isCloseTo(7.5d, withinPercentage(1));
        assertThat(pc99).isCloseTo(7.99d, withinPercentage(1));
        assertThat(pc100).isCloseTo(8, withinPercentage(0));
    }

    @Test
    public void shouldReturnMultiplePercentile2() {
        // Given
        List<Integer> values = List.of(6, 8, 9, 13, 18, 25, 39, 60, 89);

        // When
        Double pc0 = FileReferencesStats.percentile(values, 0d);
        Double pc50 = FileReferencesStats.percentile(values, 0.5d);
        Double pc99 = FileReferencesStats.percentile(values, 0.99d);
        Double pc100 = FileReferencesStats.percentile(values, 1d);

        // Then
        assertThat(pc0).isCloseTo(6, withinPercentage(0));
        assertThat(pc50).isCloseTo(18d, withinPercentage(0));
        assertThat(pc99).isCloseTo(86.68d, withinPercentage(1));
        assertThat(pc100).isCloseTo(89, withinPercentage(0));
    }
}
