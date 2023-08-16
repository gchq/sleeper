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

package sleeper.core.record.process.status;

import java.time.Instant;

public class TimeWindowQuery {
    private final Instant windowStartTime;
    private final Instant windowEndTime;

    public TimeWindowQuery(Instant windowStartTime, Instant windowEndTime) {
        this.windowStartTime = windowStartTime;
        this.windowEndTime = windowEndTime;
    }

    public boolean isStartedInWindow(Instant startTime) {
        return startTime.isAfter(windowStartTime) &&
                startTime.isBefore(windowEndTime);
    }

    public boolean isFinishedInWindow(Instant startTime, Instant endTime) {
        return startTime.isBefore(windowStartTime) &&
                endTime.isAfter(windowStartTime) &&
                endTime.isBefore(windowEndTime);
    }

    public boolean isRunningInWindow(Instant startTime) {
        return startTime.isBefore(windowStartTime);
    }

    public boolean isInWindow(Instant startTime, Instant endTime) {
        return isStartedInWindow(startTime) ||
                isRunningInWindow(startTime) ||
                isFinishedInWindow(startTime, endTime);
    }
}
