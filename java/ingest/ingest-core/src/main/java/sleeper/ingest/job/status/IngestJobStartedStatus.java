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

import sleeper.ingest.job.IngestJob;

import java.time.Instant;
import java.util.Objects;

/**
 * A status for ingest jobs that have started.
 */
public class IngestJobStartedStatus implements IngestJobInfoStatus {

    private final int inputFileCount;
    private final Instant startTime;
    private final Instant updateTime;
    private final boolean isStartOfRun;

    private IngestJobStartedStatus(Builder builder) {
        inputFileCount = builder.inputFileCount;
        startTime = Objects.requireNonNull(builder.startTime, "startTime may not be null");
        updateTime = Objects.requireNonNull(builder.updateTime, "updateTime may not be null");
        isStartOfRun = builder.isStartOfRun;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder for this class, setting the start of run flag. This flag is used to separate multiple runs of a
     * job on the same task. When a status update with this flag enabled is found, it indicates that a new run has
     * been started. This also means that there is only 1 status update with this flag enabled for every run.
     * <p>
     * For ingest jobs, the job is started when an ingest task retrieves it from an SQS queue.
     * For bulk import jobs, the job starts when the bulk import starter validates it and assigns it to a Spark cluster.
     * When a bulk import job starts in the Spark cluster, that needs to not count as the start of the run, else the
     * {@link ProcessRunsBuilder} would detect that as multiple runs.
     *
     * @param  startOfRun whether this status marks the start of a job run
     * @return            the builder
     */
    public static Builder withStartOfRun(boolean startOfRun) {
        return builder().isStartOfRun(startOfRun);
    }

    public int getInputFileCount() {
        return inputFileCount;
    }

    @Override
    public Instant getUpdateTime() {
        return updateTime;
    }

    @Override
    public Instant getStartTime() {
        return startTime;
    }

    @Override
    public boolean isStartOfRun() {
        return isStartOfRun;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IngestJobStartedStatus that = (IngestJobStartedStatus) o;
        return inputFileCount == that.inputFileCount
                && isStartOfRun == that.isStartOfRun
                && Objects.equals(startTime, that.startTime)
                && Objects.equals(updateTime, that.updateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputFileCount, startTime, updateTime, isStartOfRun);
    }

    @Override
    public String toString() {
        return "IngestJobStartedStatus{" +
                "inputFileCount=" + inputFileCount +
                ", startTime=" + startTime +
                ", updateTime=" + updateTime +
                ", isStartOfRun=" + isStartOfRun +
                '}';
    }

    /**
     * Builder class for ingest job started status objects.
     */
    public static final class Builder {
        private int inputFileCount;
        private Instant startTime;
        private Instant updateTime;
        private boolean isStartOfRun = true;

        public Builder() {
        }

        /**
         * Sets the input file count using the provided ingest job.
         *
         * @param  job the ingest job
         * @return     the builder
         */
        public Builder job(IngestJob job) {
            return inputFileCount(job.getFiles().size());
        }

        /**
         * Sets the input file count.
         *
         * @param  inputFileCount the input file count
         * @return                the builder
         */
        public Builder inputFileCount(int inputFileCount) {
            this.inputFileCount = inputFileCount;
            return this;
        }

        /**
         * Sets the start time.
         *
         * @param  startTime the start time
         * @return           the builder
         */
        public Builder startTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }

        /**
         * Sets the update time.
         *
         * @param  updateTime the update time
         * @return            the builder
         */
        public Builder updateTime(Instant updateTime) {
            this.updateTime = updateTime;
            return this;
        }

        /**
         * Sets whether this status represents the start of a job run. This flag is used to separate multiple runs of a
         * job on the same task. When a status update with this flag enabled is found, it indicates that a new run has
         * been started. This also means that there is only 1 status update with this flag enabled for every run.
         * <p>
         * For ingest jobs, the job is started when an ingest task retrieves it from an SQS queue.
         * For bulk import jobs, the job starts when the bulk import starter validates it and assigns it to a Spark
         * cluster.
         * When a bulk import job starts in the Spark cluster, that needs to not count as the start of the run, else the
         * {@link ProcessRunsBuilder} would detect that as multiple runs.
         *
         * @param  isStartOfRun whether this status represents the start of a job run
         * @return              the builder
         */
        public Builder isStartOfRun(boolean isStartOfRun) {
            this.isStartOfRun = isStartOfRun;
            return this;
        }

        public IngestJobStartedStatus build() {
            return new IngestJobStartedStatus(this);
        }
    }
}
