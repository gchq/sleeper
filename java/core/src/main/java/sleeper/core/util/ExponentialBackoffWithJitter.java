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

    public ExponentialBackoffWithJitter(DoubleSupplier randomJitterFraction, Waiter waiter) {
        this.randomJitterFraction = randomJitterFraction;
        this.waiter = waiter;
    }

    public ExponentialBackoffWithJitter() {
        this(Math::random, Thread::sleep);
    }

    public void waitBeforeAttempt(int attempt) throws InterruptedException {
        if (attempt == 0) {
            return;
        }
        long waitMillis = getWaitMillisBeforeAttempt(attempt, randomJitterFraction.getAsDouble());
        LOGGER.debug("Sleeping for {} milliseconds", waitMillis);
        waiter.waitForMillis(waitMillis);
    }

    public static long getWaitMillisBeforeAttempt(int attempt, double jitterFraction) {
        // Implements exponential back-off with jitter, see
        // https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
        int sleepTimeInSeconds = (int) Math.min(120, Math.pow(2.0, attempt + 1));
        return (long) (jitterFraction * sleepTimeInSeconds * 1000L);
    }

    interface Waiter {
        void waitForMillis(long milliseconds) throws InterruptedException;
    }
}
