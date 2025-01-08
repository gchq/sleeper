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

import java.time.Instant;
import java.util.Objects;

/**
 * A test implementation of process run started update. Only used in tests.
 */
public class ProcessStartedStatus implements ProcessRunStartedUpdate {

    private final Instant updateTime;
    private final Instant startTime;

    private ProcessStartedStatus(Instant updateTime, Instant startTime) {
        this.updateTime = Objects.requireNonNull(updateTime, "updateTime may not be null");
        this.startTime = Objects.requireNonNull(startTime, "startTime may not be null");
    }

    /**
     * Creates an instance of this class.
     *
     * @param  updateTime the update time
     * @param  startTime  the start time
     * @return            an instance of this class
     */
    public static ProcessStartedStatus updateAndStartTime(Instant updateTime, Instant startTime) {
        return new ProcessStartedStatus(updateTime, startTime);
    }

    @Override
    public Instant getUpdateTime() {
        return updateTime;
    }

    @Override
    public Instant getStartTime() {
        return startTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProcessStartedStatus that = (ProcessStartedStatus) o;
        return updateTime.equals(that.updateTime) && startTime.equals(that.startTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, startTime);
    }

    @Override
    public String toString() {
        return "ProcessStartedStatus{" +
                "updateTime=" + updateTime +
                ", startTime=" + startTime +
                '}';
    }
}
