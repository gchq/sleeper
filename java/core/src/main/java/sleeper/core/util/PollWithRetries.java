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

/**
 * Polls a function until some condition is met or a maximum number of polls is reached. Waits for a constant interval
 * between polls.
 */
public class PollWithRetries {
    private static final Logger LOGGER = LoggerFactory.getLogger(PollWithRetries.class);

    private final long pollIntervalMillis;
    private final int maxPolls;
    private final PollsTracker pollsTracker;

    private PollWithRetries(long pollIntervalMillis, int maxPolls) {
        this(pollIntervalMillis, maxPolls, new TrackAttemptsPerInvocation());
    }

    private PollWithRetries(long pollIntervalMillis, int maxPolls, PollsTracker pollsTracker) {
        this.pollIntervalMillis = pollIntervalMillis;
        this.maxPolls = maxPolls;
        this.pollsTracker = pollsTracker;
    }

    /**
     * Creates an instance of this class.
     *
     * @param  pollIntervalMillis milliseconds to wait in between polls
     * @param  maxPolls           the maximum number of polls
     * @return                    an instance of {@link PollWithRetries}
     */
    public static PollWithRetries intervalAndMaxPolls(long pollIntervalMillis, int maxPolls) {
        return new PollWithRetries(pollIntervalMillis, maxPolls);
    }

    /**
     * Creates an instance of this class.
     *
     * @param  pollInterval time to wait in between polls
     * @param  timeout      the maximum amount of time to wait for, used to compute the maximum number of polls
     * @return              an instance of {@link PollWithRetries}
     */
    public static PollWithRetries intervalAndPollingTimeout(Duration pollInterval, Duration timeout) {
        long pollIntervalMillis = pollInterval.toMillis();
        long timeoutMillis = timeout.toMillis();
        return intervalAndMaxPolls(pollIntervalMillis,
                (int) LongMath.divide(timeoutMillis, pollIntervalMillis, RoundingMode.CEILING));
    }

    /**
     * Creates an instance of this class which will only poll once and never retry.
     *
     * @return an instance of {@link PollWithRetries}
     */
    public static PollWithRetries noRetries() {
        return new PollWithRetries(0, 1);
    }

    /**
     * Creates an instance of this class which will retry a fixed number of times without waiting between polls.
     *
     * @return an instance of {@link PollWithRetries}
     */
    public static PollWithRetries immediateRetries(int retries) {
        return new PollWithRetries(0, retries + 1);
    }

    /**
     * Creates an instance of this class which will make a fixed number of attempts without waiting between polls.
     *
     * @return an instance of {@link PollWithRetries}
     */
    public static PollWithRetries immediateAttempts(int attempts) {
        return new PollWithRetries(0, attempts);
    }

    /**
     * Creates an instance of this class with an overall number of attempts that will be consumed on each invocation.
     * Note that this is not thread-safe, and should only be used when all polling is done on the same thread.
     *
     * @param  poll poll settings to replicate, but with an overall number of attempts
     * @return      an instance of {@link PollWithRetries} matching the given settings
     */
    public static PollWithRetries applyMaxAttemptsOverall(PollWithRetries poll) {
        return new PollWithRetries(poll.pollIntervalMillis, poll.maxPolls, new TrackAttemptsAcrossInvocations());
    }

    /**
     * Starts polling until an exit condition is met or the maximum polls has been reached.
     *
     * @param  description          a short description to fill in the blank of "timed out waiting until _" or "expected
     *                              to find _"
     * @param  checkFinished        the exit condition
     * @throws InterruptedException if the thread was interrupted while waiting
     * @throws TimedOutException    if the maximum number of polls is reached
     * @throws CheckFailedException if configured to only poll once, and it failed
     */
    public void pollUntil(String description, BooleanSupplier checkFinished) throws InterruptedException, TimedOutException, CheckFailedException {
        int polls = pollsTracker.getPollsBeforeInvocation();
        do {
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
            polls++;
            pollsTracker.beforePoll();
        } while (!checkFinished.getAsBoolean());
    }

    /**
     * Polls a query function until the results meet an exit condition. Returns the results of the last query.
     *
     * @param  <T>                  the result type
     * @param  description          a short description to fill in the blank of "timed out waiting until _" or "expected
     *                              to find _"
     * @param  query                the query function
     * @param  condition            the exit condition to check against each query result
     * @return                      the last query result
     * @throws InterruptedException if the thread was interrupted while waiting
     * @throws TimedOutException    if the maximum number of polls is reached
     * @throws CheckFailedException if configured to only poll once, and it failed
     */
    public <T> T queryUntil(String description, Supplier<T> query, Predicate<T> condition) throws InterruptedException, TimedOutException, CheckFailedException {
        QueryTracker<T> tracker = new QueryTracker<>(query, condition);
        pollUntil(description, tracker::checkFinished);
        return tracker.lastResult;
    }

    /**
     * Checks an exit condition against results of a query. Remembers the last query result.
     *
     * @param <T> the result type
     */
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

    /**
     * Tracks the number of polls made so far. This implemented based on whether attempts should be remembered between
     * invocations.
     */
    private interface PollsTracker {
        int getPollsBeforeInvocation();

        void beforePoll();
    }

    /**
     * Does not track attempts between polling invocations, so count of polls is not shared between invocations.
     */
    private static class TrackAttemptsPerInvocation implements PollsTracker {
        @Override
        public int getPollsBeforeInvocation() {
            return 0;
        }

        @Override
        public void beforePoll() {
        }
    }

    /**
     * Tracks attempts between polling invocations, so count of attempts is across all invocations.
     */
    private static class TrackAttemptsAcrossInvocations implements PollsTracker {
        private int attempts = 0;

        @Override
        public int getPollsBeforeInvocation() {
            return attempts;
        }

        @Override
        public void beforePoll() {
            attempts++;
        }
    }

    /**
     * Thrown when the exit condition was not met and no retries were made.
     */
    public static class CheckFailedException extends RuntimeException {
        private CheckFailedException(String message) {
            super(message);
        }
    }

    /**
     * Thrown when the exit condition was not met and the maximum number of retries were made.
     */
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
