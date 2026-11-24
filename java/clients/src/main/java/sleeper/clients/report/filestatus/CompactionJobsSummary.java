/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.clients.report.filestatus;

import java.util.Optional;

/**
 * A summary of compaction jobs assigned to file references in the state store.
 */
public class CompactionJobsSummary {

    private final int numJobsAssignedTo;
    private final int onNumPartitions;
    private final int numReferencesAssigned;
    private final CompactionJobTrackerStatusCounts trackerStatusCounts;

    private CompactionJobsSummary(Builder builder) {
        numJobsAssignedTo = builder.numJobsAssignedTo;
        onNumPartitions = builder.onNumPartitions;
        numReferencesAssigned = builder.numReferencesAssigned;
        trackerStatusCounts = builder.trackerStatusCounts == null ? null : builder.trackerStatusCounts.build();
    }

    /**
     * Creates a builder to summarise compaction jobs one at a time.
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public int getNumJobsAssignedTo() {
        return numJobsAssignedTo;
    }

    public int getOnNumPartitions() {
        return onNumPartitions;
    }

    public int getNumReferencesAssigned() {
        return numReferencesAssigned;
    }

    /**
     * Retrieves counts of these compaction jobs by their status in the job tracker. This will be empty if the jobs
     * were not looked up in the job tracker.
     *
     * @return the counts, if the jobs were looked up in the job tracker
     */
    public Optional<CompactionJobTrackerStatusCounts> getTrackerStatusCounts() {
        return Optional.ofNullable(trackerStatusCounts);
    }

    /**
     * Computes the percentage of file references that are assigned to these compaction jobs.
     *
     * @param  totalFileReferences the total number of file references in the state store
     * @return                     the percentage of references assigned to these jobs
     */
    public double getPercentOfFileReferencesAssigned(long totalFileReferences) {
        return 100.0 * (double) numReferencesAssigned / (double) totalFileReferences;
    }

    /**
     * Builds a summary of compaction jobs, updated one job at a time. Jobs must be added grouped by partition, calling
     * {@link #startPartition()} before the jobs in each partition.
     */
    public static class Builder {
        private int numJobsAssignedTo;
        private int onNumPartitions;
        private int numReferencesAssigned;
        private boolean jobAddedInPartition;
        private CompactionJobTrackerStatusCounts.Builder trackerStatusCounts;

        private Builder() {
        }

        /**
         * Sets whether to count jobs by their status in the job tracker. This should only be set if the jobs were
         * looked up in the job tracker. Must be set before any jobs are added.
         *
         * @param  countTrackerStatuses true to count jobs by their status in the job tracker
         * @return                      this builder
         */
        public Builder countTrackerStatuses(boolean countTrackerStatuses) {
            trackerStatusCounts = countTrackerStatuses ? CompactionJobTrackerStatusCounts.builder() : null;
            return this;
        }

        /**
         * Starts a new partition. The partition will be counted if any jobs are added before the next one is started.
         *
         * @return this builder
         */
        public Builder startPartition() {
            jobAddedInPartition = false;
            return this;
        }

        /**
         * Adds a compaction job to the summary, in the current partition.
         *
         * @param  job the compaction job
         * @return     this builder
         */
        public Builder addJob(TableFilesCompaction job) {
            if (!jobAddedInPartition) {
                onNumPartitions++;
                jobAddedInPartition = true;
            }
            numJobsAssignedTo++;
            numReferencesAssigned += job.getFileReferences().size();
            if (trackerStatusCounts != null) {
                trackerStatusCounts.addJob(job);
            }
            return this;
        }

        public CompactionJobsSummary build() {
            return new CompactionJobsSummary(this);
        }
    }
}
