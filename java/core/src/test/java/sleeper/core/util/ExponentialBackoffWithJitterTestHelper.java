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

import sleeper.core.util.ExponentialBackoffWithJitter.Waiter;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.function.DoubleSupplier;

/**
 * Helpers to create parameters for ExponentialBackoffWithJitter in tests.
 */
public class ExponentialBackoffWithJitterTestHelper {

    private ExponentialBackoffWithJitterTestHelper() {
    }

    /**
     * Creates a fixed seed Random.
     *
     * @return a reference to {@link Random#nextDouble}
     */
    public static DoubleSupplier fixJitterSeed() {
        return new Random(0)::nextDouble;
    }

    /**
     * Creates a double supplier with no jitter.
     *
     * @return a {@link DoubleSupplier} that always returns the same number
     */
    public static DoubleSupplier noJitter() {
        return () -> 1.0;
    }

    /**
     * Creates a double supplier with a fixed jitter fraction.
     *
     * @param  fraction the fraction
     * @return          a {@link DoubleSupplier} that always returns the fraction
     */
    public static DoubleSupplier constantJitterFraction(double fraction) {
        return () -> fraction;
    }

    /**
     * Creates an implementation of a waiter that records the wait times in a list.
     *
     * @param  recordWaits the list to store wait times
     * @return             a {@link Waiter} that records wait times
     */
    public static Waiter recordWaits(List<Duration> recordWaits) {
        return millis -> recordWaits.add(Duration.ofMillis(millis));
    }

    /**
     * Creates an implementation of a waiter that performs multiple actions.
     *
     * @param  waiters actions to perform
     * @return         a {@link Waiter} that performs the given actions
     */
    public static Waiter multipleWaitActions(Waiter... waiters) {
        return millis -> {
            for (Waiter waiter : waiters) {
                waiter.waitForMillis(millis);
            }
        };
    }

    /**
     * Creates an implementation of a waiter that does nothing.
     *
     * @return a {@link Waiter} that does nothing
     */
    public static Waiter noWaits() {
        return millis -> {
        };
    }
}
