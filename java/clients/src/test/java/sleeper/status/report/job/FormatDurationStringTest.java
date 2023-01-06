/*
 * Copyright 2022 Crown Copyright
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

package sleeper.status.report.job;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FormatDurationStringTest {
    @Test
    public void shouldFormatDurationStringWithMillisecondsOnly() {
        // Given
        double seconds = 0.123;

        // When
        String formattedString = StandardProcessRunReporter.formatDurationString(seconds);

        // Then
        assertThat(formattedString).isEqualTo("0.123s");
    }

    @Test
    public void shouldFormatDurationStringWithSecondsOnly() {
        // Given
        double seconds = 1;

        // When
        String formattedString = StandardProcessRunReporter.formatDurationString(seconds);

        // Then
        assertThat(formattedString).isEqualTo("1s");
    }

    @Test
    public void shouldFormatDurationStringWithMinutesOnly() {
        // Given
        double seconds = 300;

        // When
        String formattedString = StandardProcessRunReporter.formatDurationString(seconds);

        // Then
        assertThat(formattedString).isEqualTo("5m");
    }

    @Test
    public void shouldFormatDurationStringWithHoursOnly() {
        // Given
        double seconds = 7200;

        // When
        String formattedString = StandardProcessRunReporter.formatDurationString(seconds);

        // Then
        assertThat(formattedString).isEqualTo("2h");
    }

    @Test
    public void shouldFormatDurationStringWithAllUnits() {
        // Given
        double seconds = 12345.678;

        // When
        String formattedString = StandardProcessRunReporter.formatDurationString(seconds);

        // Then
        assertThat(formattedString).isEqualTo("3h 25m 45.678s");
    }
}
