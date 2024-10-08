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

import sleeper.core.util.ExponentialBackoffWithJitter.WaitRange;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.DoubleSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.constantJitterFraction;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.fixJitterSeed;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.noJitter;

public class ExponentialBackoffWithJitterTest {

    private static final WaitRange WAIT_RANGE = WaitRange.firstAndMaxWaitCeilingSecs(4, 120);
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

    @Test
    void shouldWaitFor10AttemptsWithAdjustedWaitRangeNoJitter() throws Exception {
        // When
        makeAttempts(10,
                WaitRange.firstAndMaxWaitCeilingSecs(0.2, 30),
                noJitter());

        // Then
        assertThat(foundWaits).containsExactly(
                Duration.parse("PT0.2S"),
                Duration.parse("PT0.4S"),
                Duration.parse("PT0.8S"),
                Duration.parse("PT1.6S"),
                Duration.parse("PT3.2S"),
                Duration.parse("PT6.4S"),
                Duration.parse("PT12.8S"),
                Duration.parse("PT25.6S"),
                Duration.ofSeconds(30));
    }

    @Test
    void shouldWaitFor10AttemptsWithAdjustedWaitRange() throws Exception {
        // When
        makeAttempts(10,
                WaitRange.firstAndMaxWaitCeilingSecs(0.2, 30),
                fixJitterSeed());

        // Then
        assertThat(foundWaits).containsExactly(
                Duration.parse("PT0.146S"),
                Duration.parse("PT0.096S"),
                Duration.parse("PT0.509S"),
                Duration.parse("PT0.88S"),
                Duration.parse("PT1.912S"),
                Duration.parse("PT2.132S"),
                Duration.parse("PT4.93S"),
                Duration.parse("PT25.211S"),
                Duration.parse("PT26.375S"));
    }

    @Test
    void shouldRefuseAttemptZero() {
        ExponentialBackoffWithJitter backoff = backoff();
        assertThatThrownBy(() -> backoff.waitBeforeAttempt(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRefuseNegativeAttempt() {
        ExponentialBackoffWithJitter backoff = backoff();
        assertThatThrownBy(() -> backoff.waitBeforeAttempt(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private void makeAttempts(int attempts) throws Exception {
        makeAttempts(attempts, fixJitterSeed());
    }

    private void makeAttempts(int attempts, DoubleSupplier randomJitterFraction) throws Exception {
        makeAttempts(attempts, WAIT_RANGE, randomJitterFraction);
    }

    private void makeAttempts(
            int attempts, WaitRange waitRange, DoubleSupplier randomJitterFraction) throws Exception {
        ExponentialBackoffWithJitter backoff = new ExponentialBackoffWithJitter(
                waitRange, randomJitterFraction, ThreadSleepTestHelper.recordWaits(foundWaits));
        for (int i = 1; i <= attempts; i++) {
            backoff.waitBeforeAttempt(i);
        }
    }

    private ExponentialBackoffWithJitter backoff() {
        return new ExponentialBackoffWithJitter(WAIT_RANGE, fixJitterSeed(), ThreadSleepTestHelper.recordWaits(foundWaits));
    }
}
