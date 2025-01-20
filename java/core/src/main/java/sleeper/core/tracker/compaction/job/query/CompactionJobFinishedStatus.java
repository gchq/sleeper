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
package sleeper.core.tracker.compaction.job.query;

import sleeper.core.tracker.job.run.RecordsProcessed;
import sleeper.core.tracker.job.status.JobRunEndUpdate;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * A status update for when a compaction job has finished.
 */
public class CompactionJobFinishedStatus implements JobRunEndUpdate {

    private final Instant updateTime;
    private final Instant finishTime;
    private final RecordsProcessed recordsProcessed;
    private final Duration timeInProcess;

    private CompactionJobFinishedStatus(Builder builder) {
        updateTime = Objects.requireNonNull(builder.updateTime, "updateTime must not be null");
        finishTime = Objects.requireNonNull(builder.finishTime, "finishTime must not be null");
        recordsProcessed = Objects.requireNonNull(builder.recordsProcessed, "recordsProcessed must not be null");
        timeInProcess = builder.timeInProcess;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Instant getUpdateTime() {
        return updateTime;
    }

    @Override
    public Instant getFinishTime() {
        return finishTime;
    }

    @Override
    public RecordsProcessed getRecordsProcessed() {
        return recordsProcessed;
    }

    @Override
    public Optional<Duration> getTimeInProcess() {
        return Optional.ofNullable(timeInProcess);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, finishTime, recordsProcessed, timeInProcess);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CompactionJobFinishedStatus)) {
            return false;
        }
        CompactionJobFinishedStatus other = (CompactionJobFinishedStatus) obj;
        return Objects.equals(updateTime, other.updateTime) && Objects.equals(finishTime, other.finishTime) && Objects.equals(recordsProcessed, other.recordsProcessed)
                && Objects.equals(timeInProcess, other.timeInProcess);
    }

    @Override
    public String toString() {
        return "CompactionJobFinishedStatus{updateTime=" + updateTime + ", finishTime=" + finishTime + ", recordsProcessed=" + recordsProcessed + ", timeInProcess=" + timeInProcess + "}";
    }

    public static class Builder {
        private Instant updateTime;
        private Instant finishTime;
        private RecordsProcessed recordsProcessed;
        private Duration timeInProcess;

        private Builder() {
        }

        public Builder updateTime(Instant updateTime) {
            this.updateTime = updateTime;
            return this;
        }

        public Builder finishTime(Instant finishTime) {
            this.finishTime = finishTime;
            return this;
        }

        public Builder recordsProcessed(RecordsProcessed recordsProcessed) {
            this.recordsProcessed = recordsProcessed;
            return this;
        }

        public Builder timeInProcess(Duration timeInProcess) {
            this.timeInProcess = timeInProcess;
            return this;
        }

        public CompactionJobFinishedStatus build() {
            return new CompactionJobFinishedStatus(this);
        }
    }

}
