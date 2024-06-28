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
package sleeper.compaction.job.status;

import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessRunFinishedUpdate;

import java.time.Instant;
import java.util.Objects;

/**
 * A status update for when a compaction job has finished.
 */
public class CompactionJobFinishedStatus implements ProcessRunFinishedUpdate {

    private final Instant updateTime;
    private final RecordsProcessedSummary summary;
    private final boolean committedBySeparateUpdate;

    private CompactionJobFinishedStatus(Builder builder) {
        updateTime = Objects.requireNonNull(builder.updateTime, "updateTime must not be null");
        summary = Objects.requireNonNull(builder.summary, "summary must not be null");
        committedBySeparateUpdate = builder.committedBySeparateUpdate;
    }

    /**
     * Creates a builder for a finished status update.
     *
     * @param  updateTime the update time to set
     * @param  summary    the records processed summary to set
     * @return            a builder
     */
    public static Builder updateTimeAndSummary(Instant updateTime, RecordsProcessedSummary summary) {
        return builder().updateTime(updateTime).summary(summary);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Instant getUpdateTime() {
        return updateTime;
    }

    @Override
    public RecordsProcessedSummary getSummary() {
        return summary;
    }

    public boolean isCommittedBySeparateUpdate() {
        return committedBySeparateUpdate;
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, summary, committedBySeparateUpdate);
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
        return Objects.equals(updateTime, other.updateTime) && Objects.equals(summary, other.summary) && committedBySeparateUpdate == other.committedBySeparateUpdate;
    }

    @Override
    public String toString() {
        return "CompactionJobFinishedStatus{updateTime=" + updateTime + ", summary=" + summary + ", committedBySeparateUpdate=" + committedBySeparateUpdate + "}";
    }

    public static class Builder {
        private Instant updateTime;
        private RecordsProcessedSummary summary;
        private boolean committedBySeparateUpdate;

        private Builder() {
        }

        public Builder updateTime(Instant updateTime) {
            this.updateTime = updateTime;
            return this;
        }

        public Builder summary(RecordsProcessedSummary summary) {
            this.summary = summary;
            return this;
        }

        public Builder committedBySeparateUpdate(boolean committedBySeparateUpdate) {
            this.committedBySeparateUpdate = committedBySeparateUpdate;
            return this;
        }

        public CompactionJobFinishedStatus build() {
            return new CompactionJobFinishedStatus(this);
        }
    }

}
