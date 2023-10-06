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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class RunAndWaitIfNeeded {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunAndWaitIfNeeded.class);
    private Instant endTime;
    private final Runnable runnable;
    private final Consumer<Long> waitFn;
    private final Supplier<Instant> timeSupplier;
    private final long delayMillis;
    private boolean hasRunBefore = false;

    public RunAndWaitIfNeeded(Runnable runnable, long delayMillis) {
        this(runnable, (time) -> {
            try {
                LOGGER.info("Waiting for {} millis", time);
                Thread.sleep(time);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, Instant::now, delayMillis);
    }

    public RunAndWaitIfNeeded(Runnable runnable, Consumer<Long> waitFn, Supplier<Instant> timeSupplier, long delayMillis) {
        this.runnable = runnable;
        this.waitFn = waitFn;
        this.timeSupplier = timeSupplier;
        this.delayMillis = delayMillis;
        this.endTime = timeSupplier.get().plus(Duration.ofMillis(delayMillis));
    }

    public void run() {
        if (hasRunBefore) {
            LOGGER.info("Has run before, checking if wait needed");
            Instant currentTime = timeSupplier.get();
            LOGGER.info("Current time: {}, End time: {}", currentTime, endTime);
            if (currentTime.isBefore(endTime)) {
                waitFn.accept(Duration.between(currentTime, endTime).toMillis());
            }
        } else {
            LOGGER.info("Skipping wait for first run");
            hasRunBefore = true;
        }
        endTime = endTime.plus(Duration.ofMillis(delayMillis));
        runnable.run();
    }
}
