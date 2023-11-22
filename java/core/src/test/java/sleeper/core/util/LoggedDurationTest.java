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

package sleeper.core.util;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class LoggedDurationTest {
    @Test
    void shouldOutputDurationWithOnlySeconds() {
        // Given
        Instant startTime = Instant.parse("2023-11-21T17:10:00Z");
        Instant stopTime = Instant.parse("2023-11-21T17:10:05Z");

        // When
        String output = LoggedDuration.between(startTime, stopTime).toString();

        // Then
        assertThat(output).isEqualTo("5");
    }

    @Test
    void shouldOutputDurationWithOnlyMilliseconds() {
        // Given
        Instant startTime = Instant.parse("2023-11-21T17:10:00Z");
        Instant stopTime = Instant.parse("2023-11-21T17:10:00.123Z");

        // When
        String output = LoggedDuration.between(startTime, stopTime).toString();

        // Then
        assertThat(output).isEqualTo("0.123");
    }

    @Test
    void shouldOutputDurationWithSecondsAndMilliseconds() {
        // Given
        Instant startTime = Instant.parse("2023-11-21T17:10:00Z");
        Instant stopTime = Instant.parse("2023-11-21T17:10:05.123Z");

        // When
        String output = LoggedDuration.between(startTime, stopTime).toString();

        // Then
        assertThat(output).isEqualTo("5.123");
    }
}
