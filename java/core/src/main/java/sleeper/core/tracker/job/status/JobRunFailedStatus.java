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

import sleeper.core.tracker.job.run.JobRunTime;
import sleeper.core.tracker.job.run.RecordsProcessed;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a job failing.
 */
public class JobRunFailedStatus implements JobRunEndUpdate {

    private final Instant updateTime;
    private final Instant finishTime;
    private final List<String> failureReasons;
    private final Duration timeInProcess;

    private JobRunFailedStatus(Instant updateTime, Instant finishTime, List<String> failureReasons, Duration timeInProcess) {
        this.updateTime = Objects.requireNonNull(updateTime, "updateTime must not be null");
        this.finishTime = Objects.requireNonNull(finishTime, "finishTime must not be null");
        this.failureReasons = Objects.requireNonNull(failureReasons, "failureReasons must not be null");
        this.timeInProcess = timeInProcess;
    }

    /**
     * Creates an instance of this class.
     *
     * @param  updateTime     the update time to set
     * @param  runTime        the time spent on the process
     * @param  failureReasons reasons the process failed
     * @return                the status update
     */
    public static JobRunFailedStatus timeAndReasons(Instant updateTime, JobRunTime runTime, List<String> failureReasons) {
        return new JobRunFailedStatus(updateTime, runTime.getFinishTime(), failureReasons, runTime.getTimeInProcess());
    }

    @Override
    public Instant getUpdateTime() {
        return updateTime;
    }

    @Override
    public Instant getFinishTime() {
        return finishTime;
    }

    @Override
    public Optional<Duration> getTimeInProcess() {
        return Optional.ofNullable(timeInProcess);
    }

    @Override
    public RecordsProcessed getRecordsProcessed() {
        return RecordsProcessed.NONE;
    }

    @Override
    public boolean isSuccessful() {
        return false;
    }

    @Override
    public List<String> getFailureReasons() {
        return failureReasons;
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, finishTime, failureReasons, timeInProcess);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof JobRunFailedStatus)) {
            return false;
        }
        JobRunFailedStatus other = (JobRunFailedStatus) obj;
        return Objects.equals(updateTime, other.updateTime) && Objects.equals(finishTime, other.finishTime) && Objects.equals(failureReasons, other.failureReasons)
                && Objects.equals(timeInProcess, other.timeInProcess);
    }

    @Override
    public String toString() {
        return "JobRunFailedStatus{updateTime=" + updateTime + ", finishTime=" + finishTime + ", failureReasons=" + failureReasons + ", timeInProcess=" + timeInProcess + "}";
    }
}
