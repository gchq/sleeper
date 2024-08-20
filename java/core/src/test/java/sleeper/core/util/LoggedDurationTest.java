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

package sleeper.core.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class LoggedDurationTest {
    @Nested
    @DisplayName("Output long format")
    class LongFormat {
        @Test
        void shouldOutputDurationWithOnlySeconds() {
            // Given
            Instant startTime = Instant.parse("2023-11-21T17:10:00Z");
            Instant stopTime = Instant.parse("2023-11-21T17:10:05Z");

            // When
            String output = LoggedDuration.withFullOutput(startTime, stopTime).toString();

            // Then
            assertThat(output).isEqualTo("5 seconds");
        }

        @Test
        void shouldOutputDurationWithOnlyMilliseconds() {
            // Given
            Instant startTime = Instant.parse("2023-11-21T17:10:00Z");
            Instant stopTime = Instant.parse("2023-11-21T17:10:00.123Z");

            // When
            String output = LoggedDuration.withFullOutput(startTime, stopTime).toString();

            // Then
            assertThat(output).isEqualTo("0.123 seconds");
        }

        @Test
        void shouldOutputDurationWithSecondsAndMilliseconds() {
            // Given
            Instant startTime = Instant.parse("2023-11-21T17:10:00Z");
            Instant stopTime = Instant.parse("2023-11-21T17:10:05.123Z");

            // When
            String output = LoggedDuration.withFullOutput(startTime, stopTime).toString();

            // Then
            assertThat(output).isEqualTo("5.123 seconds");
        }

        @Test
        void shouldOutputDurationWhenMillisecondsHasTrailingZeroes() {
            // Given
            Instant startTime = Instant.parse("2023-11-21T17:10:00Z");
            Instant stopTime = Instant.parse("2023-11-21T17:10:00.100Z");

            // When
            String output = LoggedDuration.withFullOutput(startTime, stopTime).toString();

            // Then
            assertThat(output).isEqualTo("0.1 seconds");
        }

        @Test
        void shouldOutputDurationInMinutes() {
            // Given
            Instant startTime = Instant.parse("2023-11-21T17:10:00Z");
            Instant stopTime = Instant.parse("2023-11-21T17:12:05Z");

            // When
            String output = LoggedDuration.withFullOutput(startTime, stopTime).toString();

            // Then
            assertThat(output).isEqualTo("2 minutes 5 seconds");
        }

        @Test
        void shouldOutputDurationInHours() {
            // Given
            Instant startTime = Instant.parse("2023-11-21T07:00:00Z");
            Instant stopTime = Instant.parse("2023-11-21T19:34:56.789Z");

            // When
            String output = LoggedDuration.withFullOutput(startTime, stopTime).toString();

            // Then
            assertThat(output).isEqualTo("12 hours 34 minutes 56.789 seconds");
        }
    }

    @Nested
    @DisplayName("Output long format with singular or plural")
    class LongFormatSingular {

        @Test
        void shouldOutputDurationWithSingleUnits() {
            // Given
            Instant startTime = Instant.parse("2023-11-21T17:10:00Z");
            Instant stopTime = Instant.parse("2023-11-21T18:11:01Z");

            // When
            String output = LoggedDuration.withFullOutput(startTime, stopTime).toString();

            // Then
            assertThat(output).isEqualTo("1 hour 1 minute 1 second");
        }

        @Test
        void shouldOutputZeroSecondsAsPlural() {
            // Given
            Instant startTime = Instant.parse("2023-11-21T17:10:00Z");
            Instant stopTime = Instant.parse("2023-11-21T17:10:00Z");

            // When
            String output = LoggedDuration.withFullOutput(startTime, stopTime).toString();

            // Then
            assertThat(output).isEqualTo("0 seconds");
        }

        @Test
        void shouldOutputDurationWithSingleUnitsAndFraction() {
            // Given
            Instant startTime = Instant.parse("2023-11-21T17:10:00Z");
            Instant stopTime = Instant.parse("2023-11-21T18:11:01.123Z");

            // When
            String output = LoggedDuration.withFullOutput(startTime, stopTime).toString();

            // Then
            assertThat(output).isEqualTo("1 hour 1 minute 1.123 seconds");
        }

        @Test
        void shouldOutputNegativeDurationWithSingleUnits() {
            // Given
            Instant startTime = Instant.parse("2023-11-21T19:12:01Z");
            Instant stopTime = Instant.parse("2023-11-21T18:11:00Z");

            // When
            String output = LoggedDuration.withFullOutput(startTime, stopTime).toString();

            // Then
            assertThat(output).isEqualTo("-1 hour 1 minute 1 second");
        }

        @Test
        void shouldOutputNegativeDurationWithSingleUnitsAndFraction() {
            // Given
            Instant startTime = Instant.parse("2023-11-21T19:12:01.123Z");
            Instant stopTime = Instant.parse("2023-11-21T18:11:00Z");

            // When
            String output = LoggedDuration.withFullOutput(startTime, stopTime).toString();

            // Then
            assertThat(output).isEqualTo("-1 hour 1 minute 1.123 seconds");
        }
    }

    @Nested
    @DisplayName("Output short format")
    class ShortFormat {
        @Test
        void shouldOutputStringInShortFormat() {
            // Given
            Instant startTime = Instant.parse("2023-11-21T07:00:00Z");
            Instant stopTime = Instant.parse("2023-11-21T19:34:56.789Z");

            // When
            String output = LoggedDuration.withShortOutput(startTime, stopTime).toString();

            // Then
            assertThat(output).isEqualTo("12h 34m 56.789s");
        }
    }
}
