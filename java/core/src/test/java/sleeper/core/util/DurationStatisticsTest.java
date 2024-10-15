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

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class DurationStatisticsTest {
    @Test
    void shouldReportStatisticsForOneDuration() {
        // Given
        Stream<Duration> data = Stream.of(
                Duration.ofSeconds(10));

        // When / Then
        assertThat(DurationStatistics.fromIfAny(data))
                .get().hasToString("avg: 10s, min: 10s, 99%: 10s, 99.9%: 10s, max: 10s, std dev: 0s");
    }

    @Test
    void shouldReportStatisticsForMultipleDurations() {
        // Given
        Stream<Duration> data = Stream.of(
                Duration.ofSeconds(58),
                Duration.ofSeconds(59),
                Duration.ofSeconds(60),
                Duration.ofSeconds(61),
                Duration.ofSeconds(62));

        // When / Then
        assertThat(DurationStatistics.fromIfAny(data))
                .get().hasToString("avg: 1m 0s, min: 58s, 99%: 1m 2s, 99.9%: 1m 2s, max: 1m 2s, std dev: 1.414s");
    }

    @Test
    void shouldReportStatisticsForManyDurations() {
        // Given
        Stream<Duration> data = IntStream.rangeClosed(1, 7200)
                .mapToObj(Duration::ofSeconds);

        // When / Then
        assertThat(DurationStatistics.fromIfAny(data))
                .get().hasToString("avg: 1h 0.5s, min: 1s, 99%: 1h 58m 48s, 99.9%: 1h 59m 53s, max: 2h 0s, std dev: 34m 38.46s");
    }

    @Test
    void shouldReportNoData() {
        // Given
        Stream<Duration> data = Stream.of();

        // When / Then
        assertThat(DurationStatistics.fromIfAny(data))
                .isEmpty();
    }
}
