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
import sleeper.core.tracker.job.run.JobRunTime;
import sleeper.core.tracker.job.run.RecordsProcessed;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.util.List;

/**
 * A helper for creating job status updates in tests.
 */
public class JobStatusUpdateTestHelper {

    private JobStatusUpdateTestHelper() {
    }

    /**
     * Creates a job started status.
     *
     * @param  startTime the start time
     * @return           a {@link TestJobStartedStatus}
     */
    public static TestJobStartedStatus startedStatus(Instant startTime) {
        return TestJobStartedStatus.updateAndStartTime(defaultUpdateTime(startTime), startTime);
    }

    /**
     * Creates a job finished status.
     *
     * @param  startedStatus  the {@link TestJobStartedStatus}
     * @param  runDuration    the duration
     * @param  recordsRead    the number of records read
     * @param  recordsWritten the number of records written
     * @return                a {@link JobRunFinishedStatus}
     */
    public static JobRunFinishedStatus finishedStatus(
            JobRunStartedUpdate startedStatus, Duration runDuration, long recordsRead, long recordsWritten) {
        return finishedStatus(startedStatus.getStartTime(), runDuration, recordsRead, recordsWritten);
    }

    /**
     * Creates a job finished status.
     *
     * @param  startTime      the start time
     * @param  runDuration    the duration
     * @param  recordsRead    the number of records read
     * @param  recordsWritten the number of records written
     * @return                a {@link JobRunFinishedStatus}
     */
    public static JobRunFinishedStatus finishedStatus(
            Instant startTime, Duration runDuration, long recordsRead, long recordsWritten) {
        Instant finishTime = startTime.plus(runDuration);
        JobRunSummary summary = new JobRunSummary(
                new RecordsProcessed(recordsRead, recordsWritten), startTime, finishTime);
        return JobRunFinishedStatus.updateTimeAndSummary(defaultUpdateTime(finishTime), summary);
    }

    /**
     * Creates a job failed status.
     *
     * @param  startedStatus  the {@link TestJobStartedStatus}
     * @param  runDuration    the duration
     * @param  failureReasons the reasons for the failure
     * @return                a {@link JobRunFailedStatus}
     */
    public static JobRunFailedStatus failedStatus(
            JobRunStartedUpdate startedStatus, Duration runDuration, List<String> failureReasons) {
        return failedStatus(startedStatus.getStartTime(), runDuration, failureReasons);
    }

    /**
     * Creates a job failed status.
     *
     * @param  startTime      the start time
     * @param  runDuration    the duration
     * @param  failureReasons the reasons for the failure
     * @return                a {@link JobRunFailedStatus}
     */
    public static JobRunFailedStatus failedStatus(
            Instant startTime, Duration runDuration, List<String> failureReasons) {
        JobRunTime runTime = new JobRunTime(startTime, runDuration);
        return JobRunFailedStatus.timeAndReasons(defaultUpdateTime(runTime.getFinishTime()), runTime, failureReasons);
    }

    /**
     * Creates a job failed status.
     *
     * @param  runTime        the runtime information
     * @param  failureReasons the reasons for the failure
     * @return                a {@link JobRunFailedStatus}
     */
    public static JobRunFailedStatus failedStatus(
            JobRunTime runTime, List<String> failureReasons) {
        return JobRunFailedStatus.timeAndReasons(defaultUpdateTime(runTime.getFinishTime()), runTime, failureReasons);
    }

    /**
     * Creates a default update time based on a given time.
     *
     * @param  time the provided time
     * @return      the provided time with milliseconds set to 123
     */
    public static Instant defaultUpdateTime(Instant time) {
        return time.with(ChronoField.MILLI_OF_SECOND, 123);
    }
}
