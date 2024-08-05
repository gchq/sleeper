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
    private final SleepInInterval sleepInInterval;

    private PollWithRetries(Builder builder) {
        pollIntervalMillis = builder.pollIntervalMillis;
        maxPolls = builder.maxPolls;
        pollsTracker = builder.pollsTracker;
        sleepInInterval = builder.sleepInInterval;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates an instance of this class.
     *
     * @param  pollIntervalMillis milliseconds to wait in between polls
     * @param  maxPolls           the maximum number of polls
     * @return                    an instance of {@link PollWithRetries}
     */
    public static PollWithRetries intervalAndMaxPolls(long pollIntervalMillis, int maxPolls) {
        return builder().pollIntervalMillis(pollIntervalMillis).maxPolls(maxPolls).build();
    }

    /**
     * Creates an instance of this class.
     *
     * @param  pollInterval time to wait in between polls
     * @param  timeout      the maximum amount of time to wait for, used to compute the maximum number of polls
     * @return              an instance of {@link PollWithRetries}
     */
    public static PollWithRetries intervalAndPollingTimeout(Duration pollInterval, Duration timeout) {
        return builder().pollIntervalAndTimeout(pollInterval, timeout).build();
    }

    /**
     * Creates an instance of this class which will only poll once and never retry.
     *
     * @return an instance of {@link PollWithRetries}
     */
    public static PollWithRetries noRetries() {
        return builder().noRetries().build();
    }

    /**
     * Creates an instance of this class which will retry a fixed number of times without waiting between polls.
     *
     * @return an instance of {@link PollWithRetries}
     */
    public static PollWithRetries immediateRetries(int retries) {
        return builder().immediateRetries(retries).build();
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
        failIfOverMaxPolls(description, polls);
        while (!checkFinishedAndTrack(checkFinished)) {
            polls++;
            failIfOverMaxPolls(description, polls);
            sleepInInterval.sleep(pollIntervalMillis);
        }
    }

    private boolean checkFinishedAndTrack(BooleanSupplier checkFinished) {
        pollsTracker.beforePoll();
        return checkFinished.getAsBoolean();
    }

    private void failIfOverMaxPolls(String description, int polls) {
        if (polls < maxPolls) {
            return;
        }
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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PollWithRetries)) {
            return false;
        }
        PollWithRetries other = (PollWithRetries) obj;
        return pollIntervalMillis == other.pollIntervalMillis && maxPolls == other.maxPolls && Objects.equals(pollsTracker, other.pollsTracker);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pollIntervalMillis, maxPolls, pollsTracker);
    }

    @Override
    public String toString() {
        return "PollWithRetries{pollIntervalMillis=" + pollIntervalMillis + ", maxPolls=" + maxPolls + ", pollsTracker=" + pollsTracker + "}";
    }

    /**
     * Builder for polling with retries.
     */
    public static class Builder {
        private long pollIntervalMillis;
        private int maxPolls;
        private PollsTracker pollsTracker = TrackAttemptsPerInvocation.INSTANCE;
        private SleepInInterval sleepInInterval = Thread::sleep;

        /**
         * Sets the interval between polls.
         *
         * @param  pollIntervalMillis the interval in milliseconds
         * @return                    the builder
         */
        public Builder pollIntervalMillis(long pollIntervalMillis) {
            this.pollIntervalMillis = pollIntervalMillis;
            return this;
        }

        /**
         * Sets the maximum number of polls.
         *
         * @param  maxPolls the maximum number of polls
         * @return          the builder
         */
        public Builder maxPolls(int maxPolls) {
            this.maxPolls = maxPolls;
            return this;
        }

        /**
         * Sets the interval between polls and maximum number, based on a timeout period.
         *
         * @param  pollInterval the interval
         * @param  timeout      the timeout period, used to compute the maximum number of polls
         * @return              the builder
         */
        public Builder pollIntervalAndTimeout(Duration pollInterval, Duration timeout) {
            long pollIntervalMillis = pollInterval.toMillis();
            long timeoutMillis = timeout.toMillis();
            int maxPolls = (int) LongMath.divide(timeoutMillis, pollIntervalMillis, RoundingMode.CEILING);
            return pollIntervalMillis(pollIntervalMillis).maxPolls(maxPolls);
        }

        /**
         * Sets to apply the maximum number of polls across all calls to poll a method. By default the maximum number
         * of polls is only applied within a single call to poll a method.
         *
         * @return the builder
         */
        public Builder applyMaxPollsOverall() {
            pollsTracker = new TrackAttemptsAcrossInvocations();
            return this;
        }

        /**
         * Sets no interval between polls, and a given number of retries.
         *
         * @param  retries the number of retries
         * @return         the builder
         */
        public Builder immediateRetries(int retries) {
            return pollIntervalMillis(0).maxPolls(retries + 1);
        }

        /**
         * Sets to refuse any retries, so only a single poll will be made.
         *
         * @return the builder
         */
        public Builder noRetries() {
            return maxPolls(1);
        }

        /**
         * Sets the function used to sleep for the given interval in between polls. Defaults to Thread.sleep.
         *
         * @param  sleepInInterval the function
         * @return                 the builder
         */
        public Builder sleepInInterval(SleepInInterval sleepInInterval) {
            this.sleepInInterval = sleepInInterval;
            return this;
        }

        public PollWithRetries build() {
            return new PollWithRetries(this);
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

    /**
     * Sleeps for a given period of time in between polls. Usually implemented by Thread.sleep.
     */
    public interface SleepInInterval {
        void sleep(long millis) throws InterruptedException;
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

        private static final TrackAttemptsPerInvocation INSTANCE = new TrackAttemptsPerInvocation();

        @Override
        public int getPollsBeforeInvocation() {
            return 0;
        }

        @Override
        public void beforePoll() {
        }

        @Override
        public String toString() {
            return "TrackAttemptsPerInvocation{}";
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

        @Override
        public int hashCode() {
            return Objects.hash(attempts);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof TrackAttemptsAcrossInvocations)) {
                return false;
            }
            TrackAttemptsAcrossInvocations other = (TrackAttemptsAcrossInvocations) obj;
            return attempts == other.attempts;
        }

        @Override
        public String toString() {
            return "TrackAttemptsAcrossInvocations{attempts=" + attempts + "}";
        }
    }

}
