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

import sleeper.core.util.ExponentialBackoffWithJitter.Waiter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.DoubleSupplier;

import static org.assertj.core.api.Assertions.assertThat;

public class ExponentialBackoffWithJitterTest {

    private final List<Duration> foundWaits = new ArrayList<>();

    @Test
    void shouldNotWaitOnFirstAttempt() throws Exception {
        // When
        makeAttempts(1);

        // Then
        assertThat(foundWaits).isEmpty();
    }

    @Test
    void shouldWaitFor10Attempts() throws Exception {
        // When
        makeAttempts(10);

        // Then
        assertThat(foundWaits).containsExactly(
                Duration.parse("PT2.923S"),
                Duration.parse("PT1.924S"),
                Duration.parse("PT10.198S"),
                Duration.parse("PT17.613S"),
                Duration.parse("PT38.242S"),
                Duration.parse("PT39.986S"),
                Duration.parse("PT46.222S"),
                Duration.parse("PT1M58.18S"),
                Duration.parse("PT1M45.501S"));
    }

    @Test
    void shouldWaitFor10AttemptsWithNoJitter() throws Exception {
        // When
        makeAttempts(10, noJitter());

        // Then
        assertThat(foundWaits).containsExactly(
                Duration.ofSeconds(4),
                Duration.ofSeconds(8),
                Duration.ofSeconds(16),
                Duration.ofSeconds(32),
                Duration.ofSeconds(64),
                Duration.ofMinutes(2),
                Duration.ofMinutes(2),
                Duration.ofMinutes(2),
                Duration.ofMinutes(2));
    }

    @Test
    void shouldWaitFor10AttemptsWithConstantJitterFraction() throws Exception {
        // When
        makeAttempts(10, constantJitterFraction(0.5));

        // Then
        assertThat(foundWaits).containsExactly(
                Duration.ofSeconds(2),
                Duration.ofSeconds(4),
                Duration.ofSeconds(8),
                Duration.ofSeconds(16),
                Duration.ofSeconds(32),
                Duration.ofMinutes(1),
                Duration.ofMinutes(1),
                Duration.ofMinutes(1),
                Duration.ofMinutes(1));
    }

    private void makeAttempts(int attempts) throws Exception {
        makeAttempts(attempts, fixJitterSeed());
    }

    private void makeAttempts(int attempts, DoubleSupplier randomJitterFraction) throws Exception {
        ExponentialBackoffWithJitter backoff = new ExponentialBackoffWithJitter(
                randomJitterFraction, recordWaits());
        for (int i = 0; i < attempts; i++) {
            backoff.waitBeforeAttempt(i);
        }
    }

    private static DoubleSupplier fixJitterSeed() {
        return new Random(0)::nextDouble;
    }

    private static DoubleSupplier noJitter() {
        return () -> 1.0;
    }

    private static DoubleSupplier constantJitterFraction(double fraction) {
        return () -> fraction;
    }

    private Waiter recordWaits() {
        return millis -> foundWaits.add(Duration.ofMillis(millis));
    }
}
