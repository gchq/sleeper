/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.core.tracker.job.run;

import sleeper.core.tracker.job.status.JobRunEndUpdate;
import sleeper.core.tracker.job.status.JobRunStartedUpdate;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * A summary of a run of a job, including the number of rows processed over a defined time.
 */
public class JobRunSummary {

    private final RowsProcessed rowsProcessed;
    private final JobRunTime runTime;
    private final double rowsReadPerSecond;
    private final double rowsWrittenPerSecond;

    public JobRunSummary(RowsProcessed rowsProcessed, Instant startTime, Duration duration) {
        this(rowsProcessed, startTime, startTime.plus(duration), duration);
    }

    public JobRunSummary(RowsProcessed rowsProcessed, Instant startTime, Instant finishTime) {
        this(rowsProcessed, startTime, finishTime, Duration.between(startTime, finishTime));
    }

    public JobRunSummary(RowsProcessed rowsProcessed, Instant startTime, Instant finishTime, Duration timeInProcess) {
        this(rowsProcessed, new JobRunTime(startTime, finishTime, timeInProcess));
    }

    public JobRunSummary(RowsProcessed rowsProcessed, JobRunTime runTime) {
        this.rowsProcessed = Objects.requireNonNull(rowsProcessed, "rowsProcessed must not be null");
        this.runTime = Objects.requireNonNull(runTime, "runTime must not be null");
        double secondsInProcess = runTime.getTimeInProcessInSeconds();
        this.rowsReadPerSecond = rowsProcessed.getRowsRead() / secondsInProcess;
        this.rowsWrittenPerSecond = rowsProcessed.getRowsWritten() / secondsInProcess;
    }

    /**
     * Creates a job run summary from status updates held in a tracker.
     *
     * @param  startedUpdate  the started update
     * @param  finishedUpdate the finished update
     * @return                the summary
     */
    public static JobRunSummary from(JobRunStartedUpdate startedUpdate, JobRunEndUpdate finishedUpdate) {
        return from(startedUpdate.getStartTime(), finishedUpdate);
    }

    /**
     * Creates a job run summary from status updates held in a tracker.
     *
     * @param  startTime      the start time
     * @param  finishedUpdate the finished update
     * @return                the summary
     */
    public static JobRunSummary from(Instant startTime, JobRunEndUpdate finishedUpdate) {
        Instant finishTime = finishedUpdate.getFinishTime();
        JobRunTime runTime = finishedUpdate.getTimeInProcess()
                .map(timeInProcess -> new JobRunTime(startTime, finishTime, timeInProcess))
                .orElseGet(() -> new JobRunTime(startTime, finishTime));
        return new JobRunSummary(finishedUpdate.getRowsProcessed(), runTime);
    }

    /**
     * Creates an instance of this class with no rows processed, and with no duration.
     *
     * @param  startTime the start time
     * @return           an instance of this class
     */
    public static JobRunSummary noProcessingDoneAtTime(Instant startTime) {
        return new JobRunSummary(
                new RowsProcessed(0, 0),
                startTime, Duration.ZERO);
    }

    /**
     * Creates an instance of this class with no rows processed, and the given run time.
     *
     * @param  runTime the run time
     * @return         an instance of this class
     */
    public static JobRunSummary noRowsProcessed(JobRunTime runTime) {
        return new JobRunSummary(new RowsProcessed(0, 0), runTime);
    }

    public long getRowsRead() {
        return rowsProcessed.getRowsRead();
    }

    public long getRowsWritten() {
        return rowsProcessed.getRowsWritten();
    }

    public RowsProcessed getRowsProcessed() {
        return rowsProcessed;
    }

    public JobRunTime getRunTime() {
        return runTime;
    }

    public Instant getStartTime() {
        return runTime.getStartTime();
    }

    public Instant getFinishTime() {
        return runTime.getFinishTime();
    }

    public double getDurationInSeconds() {
        return runTime.getDurationInSeconds();
    }

    public Duration getDuration() {
        return runTime.getDuration();
    }

    public Duration getTimeInProcess() {
        return runTime.getTimeInProcess();
    }

    public double getRowsReadPerSecond() {
        return rowsReadPerSecond;
    }

    public double getRowsWrittenPerSecond() {
        return rowsWrittenPerSecond;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobRunSummary that = (JobRunSummary) o;
        return rowsProcessed.equals(that.rowsProcessed) && runTime.equals(that.runTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowsProcessed, runTime);
    }

    @Override
    public String toString() {
        return "JobRunSummary{" +
                "rowsProcessed=" + rowsProcessed +
                ", runTime=" + runTime +
                ", rowsReadPerSecond=" + rowsReadPerSecond +
                ", rowsWrittenPerSecond=" + rowsWrittenPerSecond +
                '}';
    }
}
