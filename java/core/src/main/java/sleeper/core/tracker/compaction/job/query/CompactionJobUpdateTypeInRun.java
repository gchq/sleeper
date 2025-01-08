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

import sleeper.core.tracker.job.status.JobRunFailedStatus;
import sleeper.core.tracker.job.status.JobStatusUpdate;
import sleeper.core.tracker.job.status.ProcessRun;

/**
 * Defines the types of updates during a run of a compaction job. A job may be run multiple times. Can also find the
 * furthest update in a run. Uses an order to find which update is the furthest, where a failure supersedes any other
 * update.
 */
public enum CompactionJobUpdateTypeInRun {
    STARTED(1, CompactionJobStatusType.IN_PROGRESS),
    FINISHED_WHEN_COMMITTED(2, CompactionJobStatusType.UNCOMMITTED),
    COMMITTED(3, CompactionJobStatusType.FINISHED),
    FAILED(4, CompactionJobStatusType.FAILED);

    private final int order;
    private final CompactionJobStatusType jobStatusTypeAfterUpdate;

    CompactionJobUpdateTypeInRun(int order, CompactionJobStatusType jobStatusTypeAfterUpdate) {
        this.order = order;
        this.jobStatusTypeAfterUpdate = jobStatusTypeAfterUpdate;
    }

    /**
     * Gets the furthest update type for a run of a compaction job.
     *
     * @param  run the run
     * @return     the update type
     */
    public static CompactionJobUpdateTypeInRun typeOfFurthestUpdateInRun(ProcessRun run) {
        FurthestUpdateTracker furthestUpdate = new FurthestUpdateTracker();
        for (JobStatusUpdate update : run.getStatusUpdates()) {
            furthestUpdate.setIfFurther(typeOfUpdateInRun(update));
        }
        return furthestUpdate.get();
    }

    public static CompactionJobUpdateTypeInRun typeOfUpdateInRun(JobStatusUpdate update) {
        if (update instanceof CompactionJobStartedStatus) {
            return STARTED;
        } else if (update instanceof CompactionJobFinishedStatus) {
            return FINISHED_WHEN_COMMITTED;
        } else if (update instanceof CompactionJobCommittedStatus) {
            return COMMITTED;
        } else if (update instanceof JobRunFailedStatus) {
            return FAILED;
        } else {
            throw new IllegalArgumentException("Unrecognised update type: " + update.getClass().getSimpleName());
        }
    }

    public CompactionJobStatusType getJobStatusTypeAfterUpdate() {
        return jobStatusTypeAfterUpdate;
    }

    /**
     * Tracks the furthest update in a run. A failure will supersede any other update.
     */
    private static class FurthestUpdateTracker {
        private CompactionJobUpdateTypeInRun furthestType;

        public void setIfFurther(CompactionJobUpdateTypeInRun newType) {
            if (furthestType == null || furthestType.order < newType.order) {
                furthestType = newType;
            }
        }

        public CompactionJobUpdateTypeInRun get() {
            return furthestType;
        }
    }
}
