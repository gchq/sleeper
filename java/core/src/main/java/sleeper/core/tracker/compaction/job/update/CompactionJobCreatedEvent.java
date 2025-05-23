/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.core.tracker.compaction.job.update;

import java.util.Objects;

/**
 * An event for when a compaction job was created, and is due to run. Used in the compaction job tracker.
 */
public class CompactionJobCreatedEvent {

    private final String jobId;
    private final String tableId;
    private final String partitionId;
    private final int inputFilesCount;

    public CompactionJobCreatedEvent(Builder builder) {
        this.jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        this.tableId = Objects.requireNonNull(builder.tableId, "tableId must not be null");
        this.partitionId = Objects.requireNonNull(builder.partitionId, "partitionId must not be null");
        this.inputFilesCount = builder.inputFilesCount;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getJobId() {
        return jobId;
    }

    public String getTableId() {
        return tableId;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public int getInputFilesCount() {
        return inputFilesCount;
    }

    /**
     * A builder for compaction job created events.
     */
    public static final class Builder {

        private String jobId;
        private String tableId;
        private String partitionId;
        private int inputFilesCount;

        private Builder() {
        }

        /**
         * Sets the compaction job ID.
         *
         * @param  jobId the compaction job ID
         * @return       the builder
         */
        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        /**
         * Sets the table ID.
         *
         * @param  tableId the table ID
         * @return         the builder
         */
        public Builder tableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        /**
         * Records the ID of the Sleeper partition the compaction will run on.
         *
         * @param  partitionId the partition ID
         * @return             the builder
         */
        public Builder partitionId(String partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        /**
         * Records the number of input files for the compaction.
         *
         * @param  inputFilesCount the number of input files
         * @return                 the builder
         */
        public Builder inputFilesCount(int inputFilesCount) {
            this.inputFilesCount = inputFilesCount;
            return this;
        }

        public CompactionJobCreatedEvent build() {
            return new CompactionJobCreatedEvent(this);
        }
    }
}
