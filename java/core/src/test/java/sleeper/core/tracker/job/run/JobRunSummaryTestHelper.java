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

import sleeper.core.tracker.job.status.JobRunStartedUpdate;

import java.time.Duration;
import java.time.Instant;

/**
 * Helper methods for creating a summary of a run of a job.
 */
public class JobRunSummaryTestHelper {

    private JobRunSummaryTestHelper() {
    }

    /**
     * Creates a job run summary.
     *
     * @param  startedUpdate  the started status update
     * @param  duration       the duration
     * @param  recordsRead    the number of records read
     * @param  recordsWritten the number of records written
     * @return                a {@link JobRunSummary}
     */
    public static JobRunSummary summary(JobRunStartedUpdate startedUpdate, Duration duration, long recordsRead, long recordsWritten) {
        return summary(startedUpdate.getStartTime(), duration, recordsRead, recordsWritten);
    }

    /**
     * Creates a job run summary.
     *
     * @param  startTime      the start time
     * @param  duration       the duration
     * @param  recordsRead    the number of records read
     * @param  recordsWritten the number of records written
     * @return                a {@link JobRunSummary}
     */
    public static JobRunSummary summary(Instant startTime, Duration duration, long recordsRead, long recordsWritten) {
        return summary(startTime, startTime.plus(duration), recordsRead, recordsWritten);
    }

    /**
     * Creates a job run summary.
     *
     * @param  startTime      the start time
     * @param  finishTime     the finish time
     * @param  recordsRead    the number of records read
     * @param  recordsWritten the number of records written
     * @return                a {@link JobRunSummary}
     */
    public static JobRunSummary summary(Instant startTime, Instant finishTime, long recordsRead, long recordsWritten) {
        return new JobRunSummary(
                new RecordsProcessed(recordsRead, recordsWritten),
                startTime, finishTime);
    }
}
