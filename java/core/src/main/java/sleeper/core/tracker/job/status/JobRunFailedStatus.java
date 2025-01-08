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

import sleeper.core.tracker.job.JobRunSummary;
import sleeper.core.tracker.job.JobRunTime;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Represents a job failing.
 */
public class JobRunFailedStatus implements JobRunEndUpdate {

    private final Instant updateTime;
    private final JobRunTime runTime;
    private final List<String> failureReasons;

    private JobRunFailedStatus(Instant updateTime, JobRunTime runTime, List<String> failureReasons) {
        this.updateTime = Objects.requireNonNull(updateTime, "updateTime must not be null");
        this.runTime = Objects.requireNonNull(runTime, "runTime must not be null");
        this.failureReasons = Objects.requireNonNull(failureReasons, "failureReasons must not be null");
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
        return new JobRunFailedStatus(updateTime, runTime, failureReasons);
    }

    @Override
    public Instant getUpdateTime() {
        return updateTime;
    }

    @Override
    public JobRunSummary getSummary() {
        return JobRunSummary.noRecordsProcessed(runTime);
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
        return Objects.hash(updateTime, runTime, failureReasons);
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
        return Objects.equals(updateTime, other.updateTime) && Objects.equals(runTime, other.runTime) && Objects.equals(failureReasons, other.failureReasons);
    }

    @Override
    public String toString() {
        return "ProcessFailedStatus{updateTime=" + updateTime + ", runTime=" + runTime + ", failureReasons=" + failureReasons + "}";
    }
}
