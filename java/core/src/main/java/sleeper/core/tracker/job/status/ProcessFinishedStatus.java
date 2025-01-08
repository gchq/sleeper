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

import sleeper.core.tracker.job.RecordsProcessedSummary;

import java.time.Instant;
import java.util.Objects;

/**
 * Represents a process finishing, and stores the records processed summary.
 */
public class ProcessFinishedStatus implements ProcessRunFinishedUpdate {

    private final Instant updateTime;
    private final RecordsProcessedSummary summary;

    private ProcessFinishedStatus(Instant updateTime, RecordsProcessedSummary summary) {
        this.updateTime = Objects.requireNonNull(updateTime, "updateTime must not be null");
        this.summary = Objects.requireNonNull(summary, "summary must not be null");
    }

    /**
     * Creates an instance of this class.
     *
     * @param  updateTime the update time to set
     * @param  summary    the records processed summary to set
     * @return            an instance of this class
     */
    public static ProcessFinishedStatus updateTimeAndSummary(Instant updateTime, RecordsProcessedSummary summary) {
        return new ProcessFinishedStatus(updateTime, summary);
    }

    public Instant getUpdateTime() {
        return updateTime;
    }

    public RecordsProcessedSummary getSummary() {
        return summary;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProcessFinishedStatus that = (ProcessFinishedStatus) o;
        return updateTime.equals(that.updateTime) && summary.equals(that.summary);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, summary);
    }

    @Override
    public String toString() {
        return "ProcessFinishedStatus{" +
                "updateTime=" + updateTime +
                ", summary=" + summary +
                '}';
    }
}
