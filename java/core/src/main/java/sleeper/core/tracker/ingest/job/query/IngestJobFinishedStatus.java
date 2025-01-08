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
package sleeper.core.tracker.ingest.job.query;

import sleeper.core.tracker.job.JobRunSummary;
import sleeper.core.tracker.job.status.JobRunEndUpdate;

import java.time.Instant;
import java.util.Objects;

/**
 * A status update for when an ingest job has finished.
 */
public class IngestJobFinishedStatus implements JobRunEndUpdate {

    private final Instant updateTime;
    private final JobRunSummary summary;
    private final int numFilesWrittenByJob;
    private final boolean committedBySeparateFileUpdates;

    private IngestJobFinishedStatus(Builder builder) {
        updateTime = Objects.requireNonNull(builder.updateTime, "updateTime must not be null");
        summary = Objects.requireNonNull(builder.summary, "summary must not be null");
        numFilesWrittenByJob = builder.numFilesWrittenByJob;
        committedBySeparateFileUpdates = builder.committedBySeparateFileUpdates;
    }

    /**
     * Creates a status update for when an ingest job has finished.
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

    public int getNumFilesWrittenByJob() {
        return numFilesWrittenByJob;
    }

    public boolean isCommittedBySeparateFileUpdates() {
        return committedBySeparateFileUpdates;
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, summary, numFilesWrittenByJob, committedBySeparateFileUpdates);
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
        return Objects.equals(updateTime, other.updateTime) && Objects.equals(summary, other.summary) && Objects.equals(numFilesWrittenByJob, other.numFilesWrittenByJob)
                && committedBySeparateFileUpdates == other.committedBySeparateFileUpdates;
    }

    @Override
    public String toString() {
        return "IngestJobFinishedStatus{updateTime=" + updateTime + ", summary=" + summary + ", numFilesWrittenByJob=" + numFilesWrittenByJob + ", committedBySeparateFileUpdates="
                + committedBySeparateFileUpdates
                + "}";
    }

    /**
     * Builder to create the status update.
     */
    public static class Builder {
        private Instant updateTime;
        private JobRunSummary summary;
        private int numFilesWrittenByJob;
        private boolean committedBySeparateFileUpdates;

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
        public Builder summary(JobRunSummary summary) {
            this.summary = summary;
            return this;
        }

        /**
         * Sets the number of files written by the job.
         *
         * @param  numFilesWrittenByJob the number of files
         * @return                      the builder for chaining
         */
        public Builder numFilesWrittenByJob(int numFilesWrittenByJob) {
            this.numFilesWrittenByJob = numFilesWrittenByJob;
            return this;
        }

        /**
         * Sets whether the job has separate updates for when files are added to the state store. If so, the job will
         * only be finished when all files are committed. If not, the finished status update fully completes the job.
         *
         * @param  committedBySeparateFileUpdates true if the job is committed by separate updates to add files
         * @return                                the builder for chaining
         */
        public Builder committedBySeparateFileUpdates(boolean committedBySeparateFileUpdates) {
            this.committedBySeparateFileUpdates = committedBySeparateFileUpdates;
            return this;
        }

        public IngestJobFinishedStatus build() {
            return new IngestJobFinishedStatus(this);
        }
    }

}
