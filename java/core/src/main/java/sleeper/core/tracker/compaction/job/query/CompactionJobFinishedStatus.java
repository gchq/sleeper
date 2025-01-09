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

import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.status.JobRunEndUpdate;

import java.time.Instant;
import java.util.Objects;

/**
 * A status update for when a compaction job has finished.
 */
public class CompactionJobFinishedStatus implements JobRunEndUpdate {

    private final Instant updateTime;
    private final JobRunSummary summary;

    private CompactionJobFinishedStatus(Builder builder) {
        updateTime = Objects.requireNonNull(builder.updateTime, "updateTime must not be null");
        summary = Objects.requireNonNull(builder.summary, "summary must not be null");
    }

    /**
     * Creates a builder for a finished status update.
     *
     * @param  updateTime the update time to set
     * @param  summary    the records processed summary to set
     * @return            a builder
     */
    public static Builder updateTimeAndSummary(Instant updateTime, JobRunSummary summary) {
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
    public JobRunSummary getSummary() {
        return summary;
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, summary);
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
        return Objects.equals(updateTime, other.updateTime) && Objects.equals(summary, other.summary);
    }

    @Override
    public String toString() {
        return "CompactionJobFinishedStatus{updateTime=" + updateTime + ", summary=" + summary + "}";
    }

    public static class Builder {
        private Instant updateTime;
        private JobRunSummary summary;

        private Builder() {
        }

        public Builder updateTime(Instant updateTime) {
            this.updateTime = updateTime;
            return this;
        }

        public Builder summary(JobRunSummary summary) {
            this.summary = summary;
            return this;
        }

        public CompactionJobFinishedStatus build() {
            return new CompactionJobFinishedStatus(this);
        }
    }

}
