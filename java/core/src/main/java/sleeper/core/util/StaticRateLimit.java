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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Avoids a rate limit by tracking the last result for a request. Will only repeat the request within the rate limit.
 *
 * @param <T> the request result type
 */
public class StaticRateLimit<T> {

    private final Duration waitBetweenRequests;
    private final AtomicReference<LastResult> lastResult = new AtomicReference<>();
    private final Supplier<Instant> timeSupplier;

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
     * Makes the request if the expected amount of time has passed since the last request.
     *
     * @param  request the request
     * @return         the result, or the last result if the time has not passed
     */
    public T requestOrGetLast(Supplier<T> request) {
        return lastResult.updateAndGet(last -> {
            if (last == null || last.isRequestAgainAt(timeSupplier.get())) {
                return new LastResult(request.get(), timeSupplier.get());
            } else {
                return last;
            }
        }).result;
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
