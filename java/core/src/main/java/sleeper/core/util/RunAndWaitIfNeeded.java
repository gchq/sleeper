/*
 * Copyright 2022-2023 Crown Copyright
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class RunAndWaitIfNeeded {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunAndWaitIfNeeded.class);
    private Instant endTime;
    private final Consumer<Long> waitFn;
    private final Supplier<Instant> timeSupplier;
    private final long delayMillis;
    private boolean hasRunBefore = false;

    @SuppressFBWarnings("MC_OVERRIDABLE_METHOD_CALL_IN_CONSTRUCTOR")
    public RunAndWaitIfNeeded(long delayMillis) {
        this((time) -> {
            try {
                Thread.sleep(time);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, Instant::now, delayMillis);
    }

    public RunAndWaitIfNeeded(Consumer<Long> waitFn, Supplier<Instant> timeSupplier, long delayMillis) {
        this.waitFn = waitFn;
        this.timeSupplier = timeSupplier;
        this.delayMillis = delayMillis;
    }

    public void run(Runnable runnable) {
        Instant currentTime = timeSupplier.get();
        if (hasRunBefore) {
            LOGGER.info("Has run before, checking if wait needed");
            LOGGER.info("Current time: {}, End time: {}", currentTime, endTime);
            if (currentTime.isBefore(endTime)) {
                Duration waitDuration = Duration.between(currentTime, endTime);
                LOGGER.info("Waiting for {} seconds", waitDuration.toSeconds());
                waitFn.accept(waitDuration.toMillis());
            } else {
                LOGGER.info("Wait not needed as current time is after end time");
            }
            endTime = endTime.plus(Duration.ofMillis(delayMillis));
        } else {
            LOGGER.info("Skipping wait for first run");
            hasRunBefore = true;
            endTime = currentTime.plus(Duration.ofMillis(delayMillis));
        }
        runnable.run();
    }
}
