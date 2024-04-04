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

import java.util.function.DoubleSupplier;

public class ExponentialBackoffWithJitter {
    public static final Logger LOGGER = LoggerFactory.getLogger(ExponentialBackoffWithJitter.class);

    private final DoubleSupplier randomJitterFraction;
    private final Waiter waiter;
    private final WaitRange waitRange;

    public ExponentialBackoffWithJitter(WaitRange waitRange, DoubleSupplier randomJitterFraction, Waiter waiter) {
        this.waitRange = waitRange;
        this.randomJitterFraction = randomJitterFraction;
        this.waiter = waiter;
    }

    public ExponentialBackoffWithJitter(WaitRange waitRange) {
        this(waitRange, Math::random, Thread::sleep);
    }

    public void waitBeforeAttempt(int attempt) throws InterruptedException {
        if (attempt == 0) {
            return;
        }
        long waitMillis = getWaitMillisBeforeAttempt(attempt);
        LOGGER.debug("Sleeping for {} milliseconds", waitMillis);
        waiter.waitForMillis(waitMillis);
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
