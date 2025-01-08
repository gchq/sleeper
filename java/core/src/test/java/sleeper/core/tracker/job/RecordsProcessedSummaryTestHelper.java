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

import sleeper.core.tracker.job.status.ProcessRunStartedUpdate;

import java.time.Duration;
import java.time.Instant;

/**
 * Helper methods for creating a records processed summary.
 */
public class RecordsProcessedSummaryTestHelper {

    private RecordsProcessedSummaryTestHelper() {
    }

    /**
     * Creates a record processed summary.
     *
     * @param  startedUpdate  the started status update
     * @param  duration       the duration
     * @param  recordsRead    the number of records read
     * @param  recordsWritten the number of records written
     * @return                a {@link RecordsProcessedSummary}
     */
    public static RecordsProcessedSummary summary(ProcessRunStartedUpdate startedUpdate, Duration duration, long recordsRead, long recordsWritten) {
        return summary(startedUpdate.getStartTime(), duration, recordsRead, recordsWritten);
    }

    /**
     * Creates a record processed summary.
     *
     * @param  startTime      the start time
     * @param  duration       the duration
     * @param  recordsRead    the number of records read
     * @param  recordsWritten the number of records written
     * @return                a {@link RecordsProcessedSummary}
     */
    public static RecordsProcessedSummary summary(Instant startTime, Duration duration, long recordsRead, long recordsWritten) {
        return summary(startTime, startTime.plus(duration), recordsRead, recordsWritten);
    }

    /**
     * Creates a record processed summary.
     *
     * @param  startTime      the start time
     * @param  finishTime     the finish time
     * @param  recordsRead    the number of records read
     * @param  recordsWritten the number of records written
     * @return                a {@link RecordsProcessedSummary}
     */
    public static RecordsProcessedSummary summary(Instant startTime, Instant finishTime, long recordsRead, long recordsWritten) {
        return new RecordsProcessedSummary(
                new RecordsProcessed(recordsRead, recordsWritten),
                startTime, finishTime);
    }
}
