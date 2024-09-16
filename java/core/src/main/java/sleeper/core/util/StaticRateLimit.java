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

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

/**
 * Avoids a rate limit by tracking the last result for a request. Will only repeat the request within the rate limit.
 * This can be used thread safely as a static constant.
 *
 * @param <T> the request result type
 */
public class StaticRateLimit<T> {

    private final Duration waitBetweenRequests;
    private final Supplier<Instant> timeSupplier;
    private LastResult lastResult;

    private StaticRateLimit(Duration waitBetweenRequests, Supplier<Instant> timeSupplier) {
        this.waitBetweenRequests = waitBetweenRequests;
        this.timeSupplier = timeSupplier;
    }

    /**
     * Creates an instance of this class to operate at the given maximum rate per second.
     *
     * @param  <T>           the request result type
     * @param  ratePerSecond the number of requests per second to stay below
     * @return               the new instance
     */
    public static <T> StaticRateLimit<T> forMaximumRatePerSecond(double ratePerSecond) {
        return withWaitBetweenRequests(RateLimitUtils.calculateSleepForSustainedRatePerSecond(ratePerSecond), Instant::now);
    }

    /**
     * Creates an instance of this class to wait for the given time between requests.
     *
     * @param  <T>                 the request result type
     * @param  waitBetweenRequests the time to wait for between requests
     * @return                     the new instance
     */
    public static <T> StaticRateLimit<T> withWaitBetweenRequests(Duration waitBetweenRequests, Supplier<Instant> timeSupplier) {
        return new StaticRateLimit<>(waitBetweenRequests, timeSupplier);
    }

    /**
     * Creates an instance of this class which will always make the request.
     *
     * @param  <T> the request result type
     * @return     the new instance
     */
    public static <T> StaticRateLimit<T> none() {
        return withWaitBetweenRequests(Duration.ZERO, () -> Instant.MIN);
    }

    /**
     * Makes the request if the expected amount of time has passed since the last request. If a request is ongoing,
     * wait for that first.
     *
     * @param  request the request
     * @return         the result, or the last result if the time has not passed
     */
    public synchronized T requestOrGetLast(Supplier<T> request) {
        if (lastResult == null || lastResult.isRequestAgainAt(timeSupplier.get())) {
            lastResult = new LastResult(request.get(), timeSupplier.get());
        }
        return lastResult.result;
    }

    /**
     * Tracks the last result of the request.
     */
    private class LastResult {

        private final T result;
        private final Instant resultTime;

        LastResult(T result, Instant resultTime) {
            this.result = result;
            this.resultTime = resultTime;
        }

        boolean isRequestAgainAt(Instant timeNow) {
            return Duration.between(resultTime, timeNow).compareTo(waitBetweenRequests) >= 0;
        }

    }

}
