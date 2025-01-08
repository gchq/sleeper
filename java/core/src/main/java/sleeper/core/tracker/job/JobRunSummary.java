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
 * A summary of a run of a job, including the number of records processed over a defined time.
 */
public class JobRunSummary {

    private final RecordsProcessed recordsProcessed;
    private final ProcessRunTime runTime;
    private final double recordsReadPerSecond;
    private final double recordsWrittenPerSecond;

    public JobRunSummary(RecordsProcessed recordsProcessed, Instant startTime, Duration duration) {
        this(recordsProcessed, startTime, startTime.plus(duration), duration);
    }

    public JobRunSummary(RecordsProcessed recordsProcessed, Instant startTime, Instant finishTime) {
        this(recordsProcessed, startTime, finishTime, Duration.between(startTime, finishTime));
    }

    public JobRunSummary(RecordsProcessed recordsProcessed, Instant startTime, Instant finishTime, Duration timeInProcess) {
        this(recordsProcessed, new ProcessRunTime(startTime, finishTime, timeInProcess));
    }

    public JobRunSummary(RecordsProcessed recordsProcessed, ProcessRunTime runTime) {
        this.recordsProcessed = Objects.requireNonNull(recordsProcessed, "recordsProcessed must not be null");
        this.runTime = Objects.requireNonNull(runTime, "runTime must not be null");
        double secondsInProcess = runTime.getTimeInProcessInSeconds();
        this.recordsReadPerSecond = recordsProcessed.getRecordsRead() / secondsInProcess;
        this.recordsWrittenPerSecond = recordsProcessed.getRecordsWritten() / secondsInProcess;
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
    public static JobRunSummary noRecordsProcessed(ProcessRunTime runTime) {
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

    public ProcessRunTime getRunTime() {
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
