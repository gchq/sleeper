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

import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatusType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Counts of compaction jobs assigned to file references, by their status in the job tracker.
 */
public class CompactionJobTrackerStatusCounts {

    private static final List<CompactionJobStatusType> REPORT_ORDER = Stream.concat(
            Stream.of(CompactionJobStatusType.values()).filter(statusType -> statusType != CompactionJobStatusType.FAILED),
            Stream.of(CompactionJobStatusType.FAILED))
            .toList();

    private final Map<CompactionJobStatusType, Integer> numJobsByStatus;
    private final int numJobsNotInTracker;

    private CompactionJobTrackerStatusCounts(Map<CompactionJobStatusType, Integer> numJobsByStatus, int numJobsNotInTracker) {
        this.numJobsByStatus = Collections.unmodifiableMap(numJobsByStatus);
        this.numJobsNotInTracker = numJobsNotInTracker;
    }

    /**
     * Creates a builder to count compaction jobs by their status in the job tracker, one job at a time.
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Retrieves the number of jobs with each status in the job tracker. Includes every status type, in the order
     * they are defined in {@link CompactionJobStatusType}, except that failed jobs come last.
     *
     * @return the number of jobs by status
     */
    public Map<CompactionJobStatusType, Integer> getNumJobsByStatus() {
        return numJobsByStatus;
    }

    /**
     * Retrieves the number of jobs with a given status in the job tracker.
     *
     * @param  statusType the status type
     * @return            the number of jobs with that status
     */
    public int getNumJobsWithStatus(CompactionJobStatusType statusType) {
        return numJobsByStatus.get(statusType);
    }

    public int getNumJobsNotInTracker() {
        return numJobsNotInTracker;
    }

    /**
     * Counts compaction jobs by their status in the job tracker, updated one job at a time.
     */
    public static class Builder {
        private final Map<CompactionJobStatusType, Integer> numJobsByStatus = new LinkedHashMap<>();
        private int numJobsNotInTracker;

        private Builder() {
            for (CompactionJobStatusType statusType : REPORT_ORDER) {
                numJobsByStatus.put(statusType, 0);
            }
        }

        /**
         * Adds a compaction job to the counts.
         *
         * @param  job the compaction job, which must have been looked up in the job tracker
         * @return     this builder
         */
        public Builder addJob(TableFilesCompaction job) {
            Optional<CompactionJobStatus> status = job.getTrackedStatus();
            if (status.isPresent()) {
                numJobsByStatus.merge(status.get().getFurthestStatusType(), 1, Integer::sum);
            } else {
                numJobsNotInTracker++;
            }
            return this;
        }

        public CompactionJobTrackerStatusCounts build() {
            return new CompactionJobTrackerStatusCounts(new LinkedHashMap<>(numJobsByStatus), numJobsNotInTracker);
        }
    }
}
