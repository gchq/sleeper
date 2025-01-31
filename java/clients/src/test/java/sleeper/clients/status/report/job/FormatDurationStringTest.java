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

package sleeper.clients.status.report.job;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

public class FormatDurationStringTest {
    @Test
    public void shouldFormatDurationStringWithMillisecondsOnly() {
        // Given
        Duration durationMillis = Duration.ofMillis(123);

        // When
        String formattedString = StandardJobRunReporter.formatDurationString(durationMillis);

        // Then
        assertThat(formattedString).isEqualTo("0.123s");
    }

    @Test
    public void shouldFormatDurationStringWithSecondsOnly() {
        // Given
        Duration durationSecondsOnly = Duration.ofMillis(1000);

        // When
        String formattedString = StandardJobRunReporter.formatDurationString(durationSecondsOnly);

        // Then
        assertThat(formattedString).isEqualTo("1s");
    }

    @Test
    public void shouldFormatDurationStringWithMinutesOnly() {
        // Given
        Duration durationMinutesOnly = Duration.ofMillis(300000);

        // When
        String formattedString = StandardJobRunReporter.formatDurationString(durationMinutesOnly);

        // Then
        assertThat(formattedString).isEqualTo("5m");
    }

    @Test
    public void shouldFormatDurationStringWithHoursOnly() {
        // Given
        Duration durationHoursOnly = Duration.ofMillis(7200000);

        // When
        String formattedString = StandardJobRunReporter.formatDurationString(durationHoursOnly);

        // Then
        assertThat(formattedString).isEqualTo("2h");
    }

    @Test
    public void shouldFormatDurationStringWithAllUnits() {
        // Given
        Duration durationAllUnits = Duration.ofMillis(12345678);

        // When
        String formattedString = StandardJobRunReporter.formatDurationString(durationAllUnits);

        // Then
        assertThat(formattedString).isEqualTo("3h 25m 45.678s");
    }
}
