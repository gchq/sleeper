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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Utilities to avoid rate limits in a remote API.
 */
public class RateLimitUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(RateLimitUtils.class);

    private RateLimitUtils() {
    }

    /**
     * Sleeps for a duration in order to achieve a target rate.
     *
     * @param ratePerSecond the target rate per second
     */
    public static void sleepForSustainedRatePerSecond(double ratePerSecond) {
        sleepForSustainedRatePerSecond(ratePerSecond, Thread::sleep);
    }

    /**
     * Sleeps for a duration in order to achieve a target rate.
     *
     * @param ratePerSecond the target rate per second
     * @param waiter        a reference to Thread.sleep or a test fake
     */
    public static void sleepForSustainedRatePerSecond(double ratePerSecond, ThreadSleep waiter) {
        try {
            long millisecondsToSleep = calculateMillisSleepForSustainedRatePerSecond(ratePerSecond);
            LOGGER.trace("Sleeping for {} ", millisecondsToSleep);
            waiter.waitForMillis(millisecondsToSleep);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Calculates how long to sleep in milliseconds in order to achieve the target rate per second.
     *
     * @param  ratePerSecond the target rate per second
     * @return               the number of milliseconds to sleep for
     */
    public static long calculateMillisSleepForSustainedRatePerSecond(double ratePerSecond) {
        return (long) Math.ceil(1000.0 / ratePerSecond);
    }

    /**
     * Calculates how long to sleep in order to achieve the target rate per second.
     *
     * @param  ratePerSecond the target rate per second
     * @return               the duration to sleep for
     */
    public static Duration calculateSleepForSustainedRatePerSecond(double ratePerSecond) {
        return Duration.ofMillis(calculateMillisSleepForSustainedRatePerSecond(ratePerSecond));
    }
}
