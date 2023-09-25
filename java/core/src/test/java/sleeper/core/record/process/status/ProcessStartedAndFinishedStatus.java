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

import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Instant;
import java.util.Objects;

public class ProcessStartedAndFinishedStatus implements ProcessRunStartedUpdate, ProcessRunFinishedUpdate {

    private final Instant updateTime;
    private final RecordsProcessedSummary summary;

    private ProcessStartedAndFinishedStatus(Instant updateTime, RecordsProcessedSummary summary) {
        this.updateTime = Objects.requireNonNull(updateTime, "updateTime may not be null");
        this.summary = Objects.requireNonNull(summary, "summary may not be null");
    }

    public static ProcessStartedAndFinishedStatus updateAndSummary(Instant updateTime, RecordsProcessedSummary summary) {
        return new ProcessStartedAndFinishedStatus(updateTime, summary);
    }

    @Override
    public Instant getUpdateTime() {
        return updateTime;
    }

    @Override
    public Instant getStartTime() {
        return summary.getStartTime();
    }

    @Override
    public RecordsProcessedSummary getSummary() {
        return summary;
    }

    @Override
    public boolean isPartOfRun() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProcessStartedAndFinishedStatus that = (ProcessStartedAndFinishedStatus) o;
        return updateTime.equals(that.updateTime) && summary.equals(that.summary);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, summary);
    }

    @Override
    public String toString() {
        return "ProcessStartedAndFinishedStatus{" +
                "updateTime=" + updateTime +
                ", summary=" + summary +
                '}';
    }
}
