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
package sleeper.core.tracker.job.run;

import sleeper.core.tracker.job.status.JobRunEndUpdate;
import sleeper.core.tracker.job.status.JobRunStartedUpdate;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * A summary of a run of a job, including the number of records processed over a defined time.
 */
public class JobRunSummary {

    private final RecordsProcessed recordsProcessed;
    private final JobRunTime runTime;
    private final double recordsReadPerSecond;
    private final double recordsWrittenPerSecond;

    public JobRunSummary(RecordsProcessed recordsProcessed, Instant startTime, Duration duration) {
        this(recordsProcessed, startTime, startTime.plus(duration), duration);
    }

    public JobRunSummary(RecordsProcessed recordsProcessed, Instant startTime, Instant finishTime) {
        this(recordsProcessed, startTime, finishTime, Duration.between(startTime, finishTime));
    }

    public JobRunSummary(RecordsProcessed recordsProcessed, Instant startTime, Instant finishTime, Duration timeInProcess) {
        this(recordsProcessed, new JobRunTime(startTime, finishTime, timeInProcess));
    }

    public JobRunSummary(RecordsProcessed recordsProcessed, JobRunTime runTime) {
        this.recordsProcessed = Objects.requireNonNull(recordsProcessed, "recordsProcessed must not be null");
        this.runTime = Objects.requireNonNull(runTime, "runTime must not be null");
        double secondsInProcess = runTime.getTimeInProcessInSeconds();
        this.recordsReadPerSecond = recordsProcessed.getRecordsRead() / secondsInProcess;
        this.recordsWrittenPerSecond = recordsProcessed.getRecordsWritten() / secondsInProcess;
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
        return new JobRunSummary(finishedUpdate.getRecordsProcessed(), runTime);
    }

    /**
     * Creates an instance of this class with no records processed, and with no duration.
     *
     * @param  startTime the start time
     * @return           an instance of this class
     */
    public static JobRunSummary noProcessingDoneAtTime(Instant startTime) {
        return new JobRunSummary(
                new RecordsProcessed(0, 0),
                startTime, Duration.ZERO);
    }

    /**
     * Creates an instance of this class with no records processed, and the given run time.
     *
     * @param  runTime the run time
     * @return         an instance of this class
     */
    public static JobRunSummary noRecordsProcessed(JobRunTime runTime) {
        return new JobRunSummary(new RecordsProcessed(0, 0), runTime);
    }

    public long getRecordsRead() {
        return recordsProcessed.getRecordsRead();
    }

    public long getRecordsWritten() {
        return recordsProcessed.getRecordsWritten();
    }

    public RecordsProcessed getRecordsProcessed() {
        return recordsProcessed;
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

    public double getRecordsReadPerSecond() {
        return recordsReadPerSecond;
    }

    public double getRecordsWrittenPerSecond() {
        return recordsWrittenPerSecond;
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
        return recordsProcessed.equals(that.recordsProcessed) && runTime.equals(that.runTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordsProcessed, runTime);
    }

    @Override
    public String toString() {
        return "JobRunSummary{" +
                "recordsProcessed=" + recordsProcessed +
                ", runTime=" + runTime +
                ", recordsReadPerSecond=" + recordsReadPerSecond +
                ", recordsWrittenPerSecond=" + recordsWrittenPerSecond +
                '}';
    }
}
