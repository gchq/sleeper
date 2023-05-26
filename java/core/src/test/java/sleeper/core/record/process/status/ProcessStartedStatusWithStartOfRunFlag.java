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
import java.util.Objects;

public class ProcessStartedStatusWithStartOfRunFlag implements ProcessRunStartedUpdate {

    private final Instant updateTime;
    private final Instant startTime;
    private final boolean isStartOfRun;

    private ProcessStartedStatusWithStartOfRunFlag(Instant updateTime, Instant startTime, boolean isStartOfRun) {
        this.updateTime = Objects.requireNonNull(updateTime, "updateTime may not be null");
        this.startTime = Objects.requireNonNull(startTime, "startTime may not be null");
        this.isStartOfRun = isStartOfRun;
    }

    public static ProcessStartedStatusWithStartOfRunFlag updateAndStartTimeNotStartOfRun(Instant updateTime, Instant startTime) {
        return new ProcessStartedStatusWithStartOfRunFlag(updateTime, startTime, false);
    }

    @Override
    public Instant getStartTime() {
        return startTime;
    }

    @Override
    public Instant getUpdateTime() {
        return updateTime;
    }

    @Override
    public boolean isStartOfRun() {
        return isStartOfRun;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProcessStartedStatusWithStartOfRunFlag that = (ProcessStartedStatusWithStartOfRunFlag) o;
        return isStartOfRun == that.isStartOfRun && Objects.equals(updateTime, that.updateTime) && Objects.equals(startTime, that.startTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, startTime, isStartOfRun);
    }

    @Override
    public String toString() {
        return "ProcessStartedStatusWithStartOfRunFlag{" +
                "updateTime=" + updateTime +
                ", startTime=" + startTime +
                ", isStartOfRun=" + isStartOfRun +
                '}';
    }
}
