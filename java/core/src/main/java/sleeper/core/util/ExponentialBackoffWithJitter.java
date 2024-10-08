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
import java.util.Objects;
import java.util.function.DoubleSupplier;

/**
 * Waits for increasing periods of time during retries. Uses exponential backoff with full jitter. See the below
 * article.
 * <p>
 * https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
 */
public class ExponentialBackoffWithJitter {
    public static final Logger LOGGER = LoggerFactory.getLogger(ExponentialBackoffWithJitter.class);

    private final DoubleSupplier randomJitterFraction;
    private final Waiter waiter;
    private final WaitRange waitRange;

    public ExponentialBackoffWithJitter(WaitRange waitRange, DoubleSupplier randomJitterFraction, Waiter waiter) {
        this.waitRange = Objects.requireNonNull(waitRange, "waitRange must not be null");
        this.randomJitterFraction = Objects.requireNonNull(randomJitterFraction, "randomJitterFraction must not be null");
        this.waiter = Objects.requireNonNull(waiter, "waiter must not be null");
    }

    public ExponentialBackoffWithJitter(WaitRange waitRange) {
        this(waitRange, Math::random, Thread::sleep);
    }

    /**
     * Waits for a time calculated from the number of attempts.
     *
     * @param  attempt              the number of the attempt that is about to be made, starting at 1
     * @return                      the number of milliseconds waited for
     * @throws InterruptedException if the current thread was interrupted
     */
    public long waitBeforeAttempt(int attempt) throws InterruptedException {
        if (attempt == 1) { // No wait on first attempt
            return 0;
        } else if (attempt < 1) {
            throw new IllegalArgumentException("Attempt count must start at 1, found " + attempt);
        }
        long waitMillis = getWaitMillisBeforeAttempt(attempt);
        LOGGER.debug("Sleeping for {}", LoggedDuration.withFullOutput(Duration.ofMillis(waitMillis)));
        waiter.waitForMillis(waitMillis);
        return waitMillis;
    }

    private long getWaitMillisBeforeAttempt(int attempt) {
        double waitCeilingInSeconds = Math.min(
                waitRange.maxWaitCeilingSecs,
                waitRange.firstWaitCeilingSecs * Math.pow(2.0, attempt - 2)); // First retry is attempt 2
        return (long) (randomJitterFraction.getAsDouble() * waitCeilingInSeconds * 1000L);
    }

    /**
     * Defines a range for the wait time ceiling. The ceiling is an amount of time that increases exponentially for
     * each retry. Jitter is then applied to this ceiling to produce the actual wait time.
     * <p>
     * The ceiling starts at a first value for the first retry, then increases exponentially until it reaches a maximum.
     */
    public static class WaitRange {
        private final double firstWaitCeilingSecs;
        private final double maxWaitCeilingSecs;

        private WaitRange(double firstWaitCeilingSecs, double maxWaitCeilingSecs) {
            this.firstWaitCeilingSecs = firstWaitCeilingSecs;
            this.maxWaitCeilingSecs = maxWaitCeilingSecs;
        }

        /**
         * Instantiates this class.
         *
         * @param  firstWaitCeilingSecs the wait ceiling in seconds for the first retry, before applying jitter
         * @param  maxWaitCeilingSecs   the maximum wait ceiling in seconds, before applying jitter
         * @return                      an instance of this class
         */
        public static WaitRange firstAndMaxWaitCeilingSecs(double firstWaitCeilingSecs, double maxWaitCeilingSecs) {
            return new WaitRange(firstWaitCeilingSecs, maxWaitCeilingSecs);
        }

        public Duration getFirstWaitCeiling() {
            return Duration.ofMillis((long) (firstWaitCeilingSecs * 1000));
        }

        public Duration getMaxWaitCeiling() {
            return Duration.ofMillis((long) (maxWaitCeilingSecs * 1000));
        }
    }
}
