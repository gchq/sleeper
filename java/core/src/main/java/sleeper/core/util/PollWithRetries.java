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

import com.google.common.math.LongMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.RoundingMode;
import java.time.Duration;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class PollWithRetries {
    private static final Logger LOGGER = LoggerFactory.getLogger(PollWithRetries.class);

    private final long pollIntervalMillis;
    private final int maxPolls;

    private PollWithRetries(long pollIntervalMillis, int maxPolls) {
        this.pollIntervalMillis = pollIntervalMillis;
        this.maxPolls = maxPolls;
    }

    public static PollWithRetries intervalAndMaxPolls(long pollIntervalMillis, int maxPolls) {
        return new PollWithRetries(pollIntervalMillis, maxPolls);
    }

    public static PollWithRetries intervalAndPollingTimeout(Duration pollInterval, Duration timeout) {
        long pollIntervalMillis = pollInterval.toMillis();
        long timeoutMillis = timeout.toMillis();
        return intervalAndMaxPolls(pollIntervalMillis,
                (int) LongMath.divide(timeoutMillis, pollIntervalMillis, RoundingMode.CEILING));
    }

    public static PollWithRetries noRetries() {
        return new PollWithRetries(0, 1);
    }

    public static PollWithRetries immediateRetries(int retries) {
        return new PollWithRetries(0, retries + 1);
    }

    public void pollUntil(String description, BooleanSupplier checkFinished) throws InterruptedException {
        int polls = 0;
        while (!checkFinished.getAsBoolean()) {
            polls++;
            if (polls >= maxPolls) {
                if (polls > 1) {
                    String message = "Timed out after " + polls + " tries waiting for " +
                            LoggedDuration.withShortOutput(Duration.ofMillis(pollIntervalMillis * polls)) +
                            " until " + description;
                    LOGGER.error(message);
                    throw new TimedOutException(message);
                } else {
                    String message = "Failed, expected to find " + description;
                    LOGGER.error(message);
                    throw new CheckFailedException(message);
                }
            }
            Thread.sleep(pollIntervalMillis);
        }
    }

    public <T> T queryUntil(String description, Supplier<T> query, Predicate<T> condition) throws InterruptedException {
        QueryTracker<T> tracker = new QueryTracker<>(query, condition);
        pollUntil(description, tracker::checkFinished);
        return tracker.lastResult;
    }

    private static class QueryTracker<T> {
        private final Supplier<T> query;
        private final Predicate<T> condition;

        private T lastResult = null;

        QueryTracker(Supplier<T> query, Predicate<T> condition) {
            this.query = query;
            this.condition = condition;
        }

        public boolean checkFinished() {
            lastResult = query.get();
            return condition.test(lastResult);
        }
    }

    public static class CheckFailedException extends RuntimeException {
        private CheckFailedException(String message) {
            super(message);
        }
    }

    public static class TimedOutException extends CheckFailedException {
        private TimedOutException(String message) {
            super(message);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PollWithRetries that = (PollWithRetries) o;
        return pollIntervalMillis == that.pollIntervalMillis && maxPolls == that.maxPolls;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pollIntervalMillis, maxPolls);
    }

    @Override
    public String toString() {
        return "PollWithRetries{" +
                "pollIntervalMillis=" + pollIntervalMillis +
                ", maxPolls=" + maxPolls +
                '}';
    }
}
