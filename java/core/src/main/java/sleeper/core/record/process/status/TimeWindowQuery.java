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

package sleeper.core.record.process.status;

import java.time.Duration;
import java.time.Instant;

/**
 * Helper class for checking if a time lies within a defined time window.
 */
public class TimeWindowQuery {
    private final Instant windowStartTime;
    private final Instant windowEndTime;
    private final Duration maxRuntime;

    public TimeWindowQuery(Instant windowStartTime, Instant windowEndTime) {
        this(windowStartTime, windowEndTime, null);
    }

    public TimeWindowQuery(Instant windowStartTime, Instant windowEndTime, Duration maxRuntime) {
        this.windowStartTime = windowStartTime;
        this.windowEndTime = windowEndTime;
        this.maxRuntime = maxRuntime;
    }

    /**
     * Checks if the start time lies within the time window. Also considers the maximum runtime of a job.
     *
     * @param  startTime the start time to check
     * @return           whether the start time lies within the time window
     */
    public boolean isUnfinishedProcessInWindow(Instant startTime) {
        if (maxRuntime != null && startTime.plus(maxRuntime).isBefore(windowStartTime)) {
            return false;
        }
        return startTime.isBefore(windowEndTime);
    }

    /**
     * Checks if the start time and the end time lie within the time window.
     *
     * @param  startTime the start time to check
     * @param  endTime   the end time to check
     * @return           whether the start and end time lie within the time window
     */
    public boolean isFinishedProcessInWindow(Instant startTime, Instant endTime) {
        return endTime.isAfter(windowStartTime) &&
                startTime.isBefore(windowEndTime);
    }
}
