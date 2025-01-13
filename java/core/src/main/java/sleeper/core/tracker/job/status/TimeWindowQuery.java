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

package sleeper.core.tracker.job.status;

import java.time.Duration;
import java.time.Instant;

/**
 * Checks if a job was running during a defined time window. If it overlaps the window at all, it is said to be in
 * the window.
 * <p>
 * This can use a maximum runtime so that if a job has not finished, it is assumed to have finished after the
 * maximum runtime elapses.
 */
public class TimeWindowQuery {
    private final Instant windowStartTime;
    private final Instant windowEndTime;
    private final Duration maxRuntime;

    public TimeWindowQuery(Instant windowStartTime, Instant windowEndTime) {
        this(windowStartTime, windowEndTime, Duration.ofDays(1));
    }

    public TimeWindowQuery(Instant windowStartTime, Instant windowEndTime, Duration maxRuntime) {
        this.windowStartTime = windowStartTime;
        this.windowEndTime = windowEndTime;
        this.maxRuntime = maxRuntime;
    }

    /**
     * Checks if an unfinished job is in the time window. If a maximum runtime was set, it will be assumed to
     * finish at the end of that maximum time.
     *
     * @param  startTime the start time to check
     * @return           true if the job is in the time window
     */
    public boolean isUnfinishedJobInWindow(Instant startTime) {
        if (maxRuntime != null && startTime.plus(maxRuntime).isBefore(windowStartTime)) {
            return false;
        }
        return startTime.isBefore(windowEndTime);
    }

    /**
     * Checks if a finished job is in the time window.
     *
     * @param  startTime the start time to check
     * @param  endTime   the end time to check
     * @return           true if the job is in the time window
     */
    public boolean isFinishedJobInWindow(Instant startTime, Instant endTime) {
        return endTime.isAfter(windowStartTime) &&
                startTime.isBefore(windowEndTime);
    }
}
