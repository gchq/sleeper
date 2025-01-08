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
package sleeper.core.tracker.job;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Records the time spent on a run of a job. The time in process can be different from the duration if the job run was
 * split over several processes.
 */
public class JobRunTime {
    private final Instant startTime;
    private final Instant finishTime;
    private final Duration duration;
    private final Duration timeInProcess;

    public JobRunTime(Instant startTime, Duration duration) {
        this(startTime, startTime.plus(duration), duration);
    }

    public JobRunTime(Instant startTime, Instant finishTime) {
        this(startTime, finishTime, Duration.between(startTime, finishTime));
    }

    public JobRunTime(Instant startTime, Instant finishTime, Duration timeInProcess) {
        this.startTime = Objects.requireNonNull(startTime, "startTime must not be null");
        this.finishTime = Objects.requireNonNull(finishTime, "finishTime must not be null");
        this.timeInProcess = Objects.requireNonNull(timeInProcess, "timeInProcess must not be null");
        this.duration = Duration.between(startTime, finishTime);
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getFinishTime() {
        return finishTime;
    }

    public Duration getDuration() {
        return duration;
    }

    public double getDurationInSeconds() {
        return duration.toMillis() / 1000.0;
    }

    public Duration getTimeInProcess() {
        return timeInProcess;
    }

    public double getTimeInProcessInSeconds() {
        return timeInProcess.toMillis() / 1000.0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTime, finishTime, timeInProcess);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof JobRunTime)) {
            return false;
        }
        JobRunTime other = (JobRunTime) obj;
        return Objects.equals(startTime, other.startTime) && Objects.equals(finishTime, other.finishTime) && Objects.equals(timeInProcess, other.timeInProcess);
    }

    @Override
    public String toString() {
        return "JobRunTime{startTime=" + startTime + ", endTime=" + finishTime + ", timeInProcess=" + timeInProcess + "}";
    }

}
