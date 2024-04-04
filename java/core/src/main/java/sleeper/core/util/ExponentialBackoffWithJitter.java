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

import java.util.Objects;
import java.util.function.DoubleSupplier;

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

    public long waitBeforeAttempt(int attempt) throws InterruptedException {
        if (attempt == 0) {
            return 0;
        }
        long waitMillis = getWaitMillisBeforeAttempt(attempt);
        LOGGER.debug("Sleeping for {} milliseconds", waitMillis);
        waiter.waitForMillis(waitMillis);
        return waitMillis;
    }

    private long getWaitMillisBeforeAttempt(int attempt) {
        // Implements exponential back-off with jitter, see
        // https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
        double sleepTimeInSeconds = Math.min(
                waitRange.maxWaitCeilingSecs,
                waitRange.firstWaitCeilingSecs * 0.5 * Math.pow(2.0, attempt));
        return (long) (randomJitterFraction.getAsDouble() * sleepTimeInSeconds * 1000L);
    }

    interface Waiter {
        void waitForMillis(long milliseconds) throws InterruptedException;
    }

    public static class WaitRange {
        private final double firstWaitCeilingSecs;
        private final double maxWaitCeilingSecs;

        private WaitRange(double firstWaitCeilingSecs, double maxWaitCeilingSecs) {
            this.firstWaitCeilingSecs = firstWaitCeilingSecs;
            this.maxWaitCeilingSecs = maxWaitCeilingSecs;
        }

        public static WaitRange firstAndMaxWaitCeilingSecs(double firstWaitCeilingSecs, double maxWaitCeilingSecs) {
            return new WaitRange(firstWaitCeilingSecs, maxWaitCeilingSecs);
        }
    }
}
