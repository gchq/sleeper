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
package sleeper.ingest.job.status;

import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessRunFinishedUpdate;

import java.time.Instant;
import java.util.Objects;

/**
 * A status update for when an ingest job has finished.
 */
public class IngestJobFinishedStatus implements ProcessRunFinishedUpdate {

    private final Instant updateTime;
    private final RecordsProcessedSummary summary;
    private final Integer numFilesAddedByJob;
    private final boolean committedWhenAllFilesAdded;

    private IngestJobFinishedStatus(Builder builder) {
        updateTime = Objects.requireNonNull(builder.updateTime, "updateTime must not be null");
        summary = Objects.requireNonNull(builder.summary, "summary must not be null");
        numFilesAddedByJob = builder.numFilesAddedByJob;
        committedWhenAllFilesAdded = builder.committedWhenAllFilesAdded;
    }

    /**
     * Creates a status update for when an ingest job has finished.
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

    public Integer getNumFilesAddedByJob() {
        return numFilesAddedByJob;
    }

    public boolean isCommittedWhenAllFilesAdded() {
        return committedWhenAllFilesAdded;
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, summary, numFilesAddedByJob, committedWhenAllFilesAdded);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof IngestJobFinishedStatus)) {
            return false;
        }
        IngestJobFinishedStatus other = (IngestJobFinishedStatus) obj;
        return Objects.equals(updateTime, other.updateTime) && Objects.equals(summary, other.summary) && Objects.equals(numFilesAddedByJob, other.numFilesAddedByJob)
                && committedWhenAllFilesAdded == other.committedWhenAllFilesAdded;
    }

    @Override
    public String toString() {
        return "IngestJobFinishedStatus{updateTime=" + updateTime + ", summary=" + summary + ", numFilesAddedByJob=" + numFilesAddedByJob + ", committedWhenAllFilesAdded=" + committedWhenAllFilesAdded
                + "}";
    }

    /**
     * Builder to create the status update.
     */
    public static class Builder {
        private Instant updateTime;
        private RecordsProcessedSummary summary;
        private Integer numFilesAddedByJob;
        private boolean committedWhenAllFilesAdded;

        private Builder() {
        }

        /**
         * Sets the update time.
         *
         * @param  updateTime the time
         * @return            the builder for chaining
         */
        public Builder updateTime(Instant updateTime) {
            this.updateTime = updateTime;
            return this;
        }

        /**
         * Sets the summary.
         *
         * @param  summary the summary
         * @return         the builder for chaining
         */
        public Builder summary(RecordsProcessedSummary summary) {
            this.summary = summary;
            return this;
        }

        /**
         * Sets the number of files added by the job.
         *
         * @param  numFilesAddedByJob the number of files
         * @return                    the builder for chaining
         */
        public Builder numFilesAddedByJob(Integer numFilesAddedByJob) {
            this.numFilesAddedByJob = numFilesAddedByJob;
            return this;
        }

        /**
         * Sets whether the job is only committed when all files have been added to the state store. If not, the
         * finished status update fully completes the job.
         *
         * @param  committedWhenAllFilesAdded true if the job is only committed after all files are added
         * @return                            the builder for chaining
         */
        public Builder committedWhenAllFilesAdded(boolean committedWhenAllFilesAdded) {
            this.committedWhenAllFilesAdded = committedWhenAllFilesAdded;
            return this;
        }

        public IngestJobFinishedStatus build() {
            return new IngestJobFinishedStatus(this);
        }
    }

}