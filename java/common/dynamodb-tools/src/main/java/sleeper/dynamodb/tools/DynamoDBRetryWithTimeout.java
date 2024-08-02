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
package sleeper.dynamodb.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.util.PollWithRetries;
import sleeper.core.util.PollWithRetries.TimedOutException;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;

public class DynamoDBRetryWithTimeout {
    public static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBRetryWithTimeout.class);
    private final long maxWaitSeconds;
    private final long maxWaitForThrottlingSeconds;
    private final Supplier<Boolean> retryCheck;
    private final Supplier<Instant> timeSupplier;
    private final Runnable delayBeforeRetry;
    private final PollWithRetries throttingRetryPoll;

    public DynamoDBRetryWithTimeout(long maxWaitSeconds, long maxWaitForThrottlingSeconds,
            Supplier<Boolean> retryCheck, Runnable delayBeforeRetry, Supplier<Instant> timeSupplier, PollWithRetries throttingRetryPoll) {
        this.maxWaitSeconds = maxWaitSeconds;
        this.maxWaitForThrottlingSeconds = maxWaitForThrottlingSeconds;
        this.retryCheck = retryCheck;
        this.delayBeforeRetry = delayBeforeRetry;
        this.timeSupplier = timeSupplier;
        this.throttingRetryPoll = throttingRetryPoll;
    }

    public <T> Instant run(ParameterSupplier<T> parameterSupplier, DynamoRunner<T> runner) throws RuntimeException, InterruptedException {
        return run(timeSupplier.get(), parameterSupplier, runner);
    }

    public <T> Instant run(Instant startTime, ParameterSupplier<T> parameterSupplier, DynamoRunner<T> runner) throws RuntimeException, InterruptedException {
        Instant lastActiveTime = startTime;
        Duration totalWaitForThrottling = Duration.ZERO;
        boolean hasThrottled = false;
        Instant currentTime = startTime;
        while (retryCheck.get()) {
            currentTime = timeSupplier.get();
            if (hasThrottled) {
                totalWaitForThrottling = totalWaitForThrottling.plus(Duration.between(lastActiveTime, currentTime));
                if (totalWaitForThrottling.compareTo(Duration.ofSeconds(maxWaitForThrottlingSeconds)) >= 0) {
                    LOGGER.info("Max wait time for throttling exceeded, exiting early");
                    return currentTime;
                }
                hasThrottled = false;
            }
            Optional<T> paramOpt = parameterSupplier.get();
            if (!paramOpt.isPresent()) {
                Duration runTime = Duration.between(lastActiveTime, currentTime);
                if (runTime.compareTo(Duration.ofSeconds(maxWaitSeconds)) >= 0) {
                    return currentTime;
                }
                delayBeforeRetry.run();
            } else {
                T parameter = paramOpt.get();
                try {
                    DynamoDBUtils.retryOnThrottlingException(throttingRetryPoll, () -> runner.run(parameter));
                } catch (TimedOutException e) {
                    LOGGER.info("Timed out waiting for throttling");
                    hasThrottled = true;
                }
                lastActiveTime = currentTime;
            }
        }
        return currentTime;
    }

    public interface ParameterSupplier<T> extends Supplier<Optional<T>> {
    }

    public interface DynamoRunner<T> {
        void run(T obj) throws RuntimeException;
    }
}
