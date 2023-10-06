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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

public class RunAndWaitIfNeededTest {

    @Test
    void shouldWaitIfCurrentTimeBeforeEndTime() {
        // Given
        AtomicBoolean hasRun = new AtomicBoolean(false);
        AtomicBoolean hasWaited = new AtomicBoolean(false);
        RunAndWaitIfNeeded runAndWaitIfNeeded = new RunAndWaitIfNeeded(
                () -> hasRun.set(true),
                (waitTime) -> hasWaited.set(true),
                List.of(Instant.parse("2023-10-06T10:56:00Z"),
                        Instant.parse("2023-10-06T10:56:01Z")).iterator()::next,
                2000L);

        // When
        runAndWaitIfNeeded.run();

        // Then
        assertThat(hasRun.get()).isTrue();
        assertThat(hasWaited.get()).isTrue();
    }

    @Test
    void shouldNotWaitIfCurrentTimeAfterEndTime() {
        // Given
        AtomicBoolean hasRun = new AtomicBoolean(false);
        AtomicBoolean hasWaited = new AtomicBoolean(false);
        RunAndWaitIfNeeded runAndWaitIfNeeded = new RunAndWaitIfNeeded(
                () -> hasRun.set(true),
                (waitTime) -> hasWaited.set(true),
                List.of(Instant.parse("2023-10-06T10:56:00Z"),
                        Instant.parse("2023-10-06T10:56:05Z")).iterator()::next,
                2000L);

        // When
        runAndWaitIfNeeded.run();

        // Then
        assertThat(hasRun.get()).isTrue();
        assertThat(hasWaited.get()).isFalse();
    }

    @Test
    void shouldCalculateNewEndTimeFromCurrentTime() {
        // Given
        AtomicBoolean hasRun = new AtomicBoolean(false);
        List<Long> waits = new ArrayList<>();
        RunAndWaitIfNeeded runAndWaitIfNeeded = new RunAndWaitIfNeeded(
                () -> hasRun.set(true),
                (waitTime) -> waits.add(waitTime),
                List.of(Instant.parse("2023-10-06T10:56:00Z"),
                        Instant.parse("2023-10-06T10:56:01Z"),
                        Instant.parse("2023-10-06T10:56:02Z")).iterator()::next,
                5000L);

        // When
        runAndWaitIfNeeded.run();
        runAndWaitIfNeeded.run();

        // Then
        assertThat(hasRun.get()).isTrue();
        assertThat(waits)
                .containsExactly(4000L, 4000L);
    }
}
