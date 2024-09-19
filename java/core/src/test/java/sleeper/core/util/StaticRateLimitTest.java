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
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class StaticRateLimitTest {

    @Test
    void shouldRequestOnce() {
        // Given
        AtomicInteger count = new AtomicInteger();
        Iterator<Instant> times = List.of(Instant.parse("2024-09-11T10:41:00Z")).iterator(); // Result time
        StaticRateLimit<Integer> limit = StaticRateLimit.withWaitBetweenRequests(Duration.ofSeconds(1), times::next);

        // When
        Integer result = limit.requestOrGetLast(count::incrementAndGet);

        // Then
        assertThat(result).isOne();
        assertThat(times).isExhausted();
    }

    @Test
    void shouldRequestTwiceWithExpectedWait() {
        // Given
        AtomicInteger count = new AtomicInteger();
        Iterator<Instant> times = List.of(
                Instant.parse("2024-09-11T10:41:00Z"), // First result time
                Instant.parse("2024-09-11T10:41:01Z"), // Second check time
                Instant.parse("2024-09-11T10:41:02Z")) // Second result time
                .iterator();
        StaticRateLimit<Integer> limit = StaticRateLimit.withWaitBetweenRequests(Duration.ofSeconds(1), times::next);

        // When
        limit.requestOrGetLast(count::incrementAndGet);
        Integer result = limit.requestOrGetLast(count::incrementAndGet);

        // Then
        assertThat(result).isEqualTo(2);
        assertThat(times).isExhausted();
    }

    @Test
    void shouldRequestOnceWhenRepeatedWithinWaitTime() {
        // Given
        AtomicInteger count = new AtomicInteger();
        Iterator<Instant> times = List.of(
                Instant.parse("2024-09-11T10:41:00Z"), // First result time
                Instant.parse("2024-09-11T10:41:00.900Z")) // Second check time
                .iterator();
        StaticRateLimit<Integer> limit = StaticRateLimit.withWaitBetweenRequests(Duration.ofSeconds(1), times::next);

        // When
        limit.requestOrGetLast(count::incrementAndGet);
        Integer result = limit.requestOrGetLast(count::incrementAndGet);

        // Then
        assertThat(result).isEqualTo(1);
        assertThat(times).isExhausted();
    }

    @Test
    void shouldWaitThenRepeat() {
        // Given
        AtomicInteger count = new AtomicInteger();
        Iterator<Instant> times = List.of(
                Instant.parse("2024-09-11T10:41:00Z"), // First result time
                Instant.parse("2024-09-11T10:41:01Z"), // Second check time
                Instant.parse("2024-09-11T10:41:02Z"), // Second result time
                Instant.parse("2024-09-11T10:41:02.900Z")) // Third check time
                .iterator();
        StaticRateLimit<Integer> limit = StaticRateLimit.withWaitBetweenRequests(Duration.ofSeconds(1), times::next);

        // When
        Integer result1 = limit.requestOrGetLast(count::incrementAndGet);
        Integer result2 = limit.requestOrGetLast(count::incrementAndGet);
        Integer result3 = limit.requestOrGetLast(count::incrementAndGet);

        // Then
        assertThat(List.of(result1, result2, result3))
                .containsExactly(1, 2, 2);
        assertThat(times).isExhausted();
    }

    @Test
    void shouldRepeatThenWait() {
        // Given
        AtomicInteger count = new AtomicInteger();
        Iterator<Instant> times = List.of(
                Instant.parse("2024-09-11T10:41:00Z"), // First result time
                Instant.parse("2024-09-11T10:41:00.900Z"), // Second check time
                Instant.parse("2024-09-11T10:41:02Z"), // Third check time
                Instant.parse("2024-09-11T10:41:03Z")) // Second result time
                .iterator();
        StaticRateLimit<Integer> limit = StaticRateLimit.withWaitBetweenRequests(Duration.ofSeconds(1), times::next);

        // When
        Integer result1 = limit.requestOrGetLast(count::incrementAndGet);
        Integer result2 = limit.requestOrGetLast(count::incrementAndGet);
        Integer result3 = limit.requestOrGetLast(count::incrementAndGet);

        // Then
        assertThat(List.of(result1, result2, result3))
                .containsExactly(1, 1, 2);
        assertThat(times).isExhausted();
    }
}
