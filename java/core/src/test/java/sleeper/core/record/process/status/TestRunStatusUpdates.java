/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.core.record.process.status;

import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Duration;
import java.time.Instant;

public class TestRunStatusUpdates {

    private TestRunStatusUpdates() {
    }

    public static ProcessStartedStatus startedStatus(Instant startTime) {
        Instant startUpdateTime = startTime.plus(Duration.ofMillis(123));
        return ProcessStartedStatus.updateAndStartTime(startUpdateTime, startTime);
    }

    public static ProcessFinishedStatus finishedStatus(
            ProcessRunStartedUpdate startedStatus, Duration runDuration, long recordsRead, long recordsWritten) {
        return finishedStatus(startedStatus.getStartTime(), runDuration, recordsRead, recordsWritten);
    }

    public static ProcessFinishedStatus finishedStatus(
            Instant startTime, Duration runDuration, long recordsRead, long recordsWritten) {
        Instant finishTime = startTime.plus(runDuration);
        Instant finishUpdateTime = finishTime.plus(Duration.ofMillis(123));
        RecordsProcessedSummary summary = new RecordsProcessedSummary(
                new RecordsProcessed(recordsRead, recordsWritten), startTime, finishTime);
        return ProcessFinishedStatus.updateTimeAndSummary(finishUpdateTime, summary);
    }
}
