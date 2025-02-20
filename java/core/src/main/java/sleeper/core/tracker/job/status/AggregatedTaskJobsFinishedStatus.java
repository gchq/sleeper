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

import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.RecordsProcessed;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents the aggregated result of job runs in a finished task.
 */
public class AggregatedTaskJobsFinishedStatus implements JobRunEndUpdate {

    private final Instant updateTime;
    private final Instant finishTime;
    private final RecordsProcessed recordsProcessed;
    private final Duration timeInProcess;

    private AggregatedTaskJobsFinishedStatus(Instant updateTime, Instant finishTime, RecordsProcessed recordsProcessed, Duration timeInProcess) {
        this.updateTime = Objects.requireNonNull(updateTime, "updateTime must not be null");
        this.finishTime = Objects.requireNonNull(finishTime, "finishTime must not be null");
        this.recordsProcessed = Objects.requireNonNull(recordsProcessed, "recordsProcessed must not be null");
        this.timeInProcess = Objects.requireNonNull(timeInProcess, "timeInProcess must not be null");
    }

    /**
     * Creates an instance of this class.
     *
     * @param  updateTime the update time to set
     * @param  summary    the records processed summary to set
     * @return            an instance of this class
     */
    public static AggregatedTaskJobsFinishedStatus updateTimeAndSummary(Instant updateTime, JobRunSummary summary) {
        return new AggregatedTaskJobsFinishedStatus(updateTime, summary.getFinishTime(), summary.getRecordsProcessed(), summary.getTimeInProcess());
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
    public RecordsProcessed getRecordsProcessed() {
        return recordsProcessed;
    }

    @Override
    public Optional<Duration> getTimeInProcess() {
        return Optional.of(timeInProcess);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof AggregatedTaskJobsFinishedStatus)) {
            return false;
        }
        AggregatedTaskJobsFinishedStatus other = (AggregatedTaskJobsFinishedStatus) obj;
        return Objects.equals(updateTime, other.updateTime) && Objects.equals(finishTime, other.finishTime) && Objects.equals(recordsProcessed, other.recordsProcessed)
                && Objects.equals(timeInProcess, other.timeInProcess);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, finishTime, recordsProcessed, timeInProcess);
    }

    @Override
    public String toString() {
        return "JobRunFinishedStatus{updateTime=" + updateTime + ", finishTime=" + finishTime + ", recordsProcessed=" + recordsProcessed + ", timeInProcess=" + timeInProcess + "}";
    }
}
