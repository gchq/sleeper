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

public class RateLimitUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(RateLimitUtils.class);

    private RateLimitUtils() {
    }

    public static void sleepForSustainedRatePerSecond(double ratePerSecond) {
        try {
            long millisecondsToSleep = calculateMillisSleepForSustainedRatePerSecond(ratePerSecond);
            LOGGER.trace("Sleeping for {} ", millisecondsToSleep);
            Thread.sleep(millisecondsToSleep);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static long calculateMillisSleepForSustainedRatePerSecond(double ratePerSecond) {
        return (long) Math.ceil(1000.0 / ratePerSecond);
    }
}
