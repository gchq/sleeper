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
package sleeper.core.tracker.job.status;

import sleeper.core.tracker.job.run.JobRunSummary;
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
     * @return                a {@link AggregatedTaskJobsFinishedStatus}
     */
    public static AggregatedTaskJobsFinishedStatus finishedStatus(
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
     * @return                a {@link AggregatedTaskJobsFinishedStatus}
     */
    public static AggregatedTaskJobsFinishedStatus finishedStatus(
            Instant startTime, Duration runDuration, long recordsRead, long recordsWritten) {
        Instant finishTime = startTime.plus(runDuration);
        JobRunSummary summary = new JobRunSummary(
                new RecordsProcessed(recordsRead, recordsWritten), startTime, finishTime);
        return AggregatedTaskJobsFinishedStatus.updateTimeAndSummary(defaultUpdateTime(finishTime), summary);
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
        Instant failureTime = startedStatus.getStartTime().plus(runDuration);
        return failedStatus(failureTime, failureReasons);
    }

    /**
     * Creates a job failed status.
     *
     * @param  failureTime    the time of the failure
     * @param  failureReasons the reasons for the failure
     * @return                a {@link JobRunFailedStatus}
     */
    public static JobRunFailedStatus failedStatus(
            Instant failureTime, List<String> failureReasons) {
        return JobRunFailedStatus.builder()
                .updateTime(defaultUpdateTime(failureTime))
                .failureTime(failureTime)
                .failureReasons(failureReasons)
                .build();
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
