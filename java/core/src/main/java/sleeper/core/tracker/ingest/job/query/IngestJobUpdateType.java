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

import sleeper.core.tracker.job.run.JobRun;
import sleeper.core.tracker.job.status.JobRunFailedStatus;
import sleeper.core.tracker.job.status.JobStatusUpdate;

/**
 * Defines the types of updates during an ingest job. Can also find the furthest update in a run of an ingest job,
 * where a job may be run multiple times. Uses an order to find which update is the furthest, where a failure supersedes
 * any other update.
 */
public enum IngestJobUpdateType {
    ACCEPTED(1, IngestJobStatusType.ACCEPTED),
    STARTED(2, IngestJobStatusType.IN_PROGRESS),
    ADDED_FILES(3, IngestJobStatusType.IN_PROGRESS),
    FINISHED_WHEN_FILES_COMMITTED(4, IngestJobStatusType.UNCOMMITTED),
    FINISHED(5, IngestJobStatusType.FINISHED),
    REJECTED(6, IngestJobStatusType.REJECTED),
    FAILED(7, IngestJobStatusType.FAILED);

    private final int order;
    private final IngestJobStatusType jobStatusTypeAfterUpdate;

    IngestJobUpdateType(int order, IngestJobStatusType jobStatusTypeAfterUpdate) {
        this.order = order;
        this.jobStatusTypeAfterUpdate = jobStatusTypeAfterUpdate;
    }

    /**
     * Gets the furthest update type for a run of an ingest job.
     *
     * @param  run the run
     * @return     the update type
     */
    public static IngestJobUpdateType typeOfFurthestUpdateInRun(JobRun run) {
        FurthestUpdateTracker furthestUpdate = new FurthestUpdateTracker();
        for (JobStatusUpdate update : run.getStatusUpdates()) {
            furthestUpdate.setIfFurther(typeOfUpdate(update));
        }
        return furthestUpdate.get();
    }

    /**
     * Finds the status type of an ingest job after an update of this type during the given run of the job.
     *
     * @param  run the run
     * @return     the status type
     */
    public IngestJobStatusType statusTypeAfterThisInRun(JobRun run) {
        if (this == FINISHED_WHEN_FILES_COMMITTED) {
            return IngestJobFilesWrittenAndAdded.from(run).haveAllFilesBeenAdded()
                    ? IngestJobStatusType.FINISHED
                    : IngestJobStatusType.UNCOMMITTED;
        } else {
            return jobStatusTypeAfterUpdate;
        }
    }

    /**
     * Gets the type of the provided process status update.
     *
     * @param  update                   the process status update
     * @return                          the type of the update
     * @throws IllegalArgumentException if the update is not of a type expected during an ingest job
     */
    public static IngestJobUpdateType typeOfUpdate(JobStatusUpdate update) {
        if (update instanceof IngestJobRejectedStatus) {
            return REJECTED;
        } else if (update instanceof IngestJobAcceptedStatus) {
            return ACCEPTED;
        } else if (update instanceof JobRunFailedStatus) {
            return FAILED;
        } else if (update instanceof IngestJobStartedStatus) {
            return STARTED;
        } else if (update instanceof IngestJobAddedFilesStatus) {
            return ADDED_FILES;
        } else if (update instanceof IngestJobFinishedStatus) {
            IngestJobFinishedStatus finished = (IngestJobFinishedStatus) update;
            if (finished.isCommittedBySeparateFileUpdates()) {
                return FINISHED_WHEN_FILES_COMMITTED;
            } else {
                return FINISHED;
            }
        } else {
            throw new IllegalArgumentException("Unrecognised status update type: " + update.getClass().getSimpleName());
        }
    }

    /**
     * Tracks the furthest update in a run. A failure will supersede any other update.
     */
    private static class FurthestUpdateTracker {
        private IngestJobUpdateType furthestType;

        public void setIfFurther(IngestJobUpdateType newType) {
            if (furthestType == null || furthestType.order < newType.order) {
                furthestType = newType;
            }
        }

        public IngestJobUpdateType get() {
            return furthestType;
        }
    }
}
