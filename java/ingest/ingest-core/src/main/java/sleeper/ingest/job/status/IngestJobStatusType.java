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

import sleeper.core.record.process.status.ProcessFailedStatus;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessStatusUpdate;

/**
 * Defines the states an ingest job can be in.
 */
public enum IngestJobStatusType {
    REJECTED(1, 5),
    ACCEPTED(2, 1),
    FAILED(3, 6),
    IN_PROGRESS(4, 2),
    UNCOMMITTED(5, 3),
    FINISHED(6, 4);

    private final int orderInJob;
    private final int orderInRun;

    IngestJobStatusType(int orderInJob, int orderInRun) {
        this.orderInJob = orderInJob;
        this.orderInRun = orderInRun;
    }

    public boolean isRunInProgress() {
        return this == ACCEPTED || this == IN_PROGRESS;
    }

    public boolean isEndOfJob() {
        return this == REJECTED || this == FINISHED;
    }

    /**
     * Gets the furthest status type for any run of an ingest job.
     *
     * @param  job the job
     * @return     the status type
     */
    public static IngestJobStatusType statusTypeOfFurthestRunOfJob(IngestJobStatus job) {
        StatusTracker furthestStatus = StatusTracker.runStatuses();
        for (ProcessRun run : job.getJobRuns()) {
            furthestStatus.setIfLater(statusTypeOfJobRun(run));
        }
        return furthestStatus.get();
    }

    /**
     * Gets the status type for a run of an ingest job.
     *
     * @param  run the run
     * @return     the status type
     */
    public static IngestJobStatusType statusTypeOfJobRun(ProcessRun run) {
        StatusTracker furthestStatus = StatusTracker.updateStatuses();
        for (ProcessStatusUpdate update : run.getStatusUpdates()) {
            furthestStatus.setIfLater(statusTypeOfUpdate(update));
        }
        return furthestStatus.get();
    }

    /**
     * Gets the status type for the provided process status update.
     *
     * @param  update the process status update
     * @return        the ingest job status type of the update
     */
    private static IngestJobStatusType statusTypeOfUpdate(ProcessStatusUpdate update) {
        if (update instanceof IngestJobRejectedStatus) {
            return REJECTED;
        } else if (update instanceof IngestJobAcceptedStatus) {
            return ACCEPTED;
        } else if (update instanceof ProcessFailedStatus) {
            return FAILED;
        } else if (update instanceof IngestJobStartedStatus) {
            return IN_PROGRESS;
        } else if (update instanceof IngestJobAddedFilesStatus) {
            return IN_PROGRESS;
        } else if (update instanceof IngestJobFinishedStatus) {
            IngestJobFinishedStatus finished = (IngestJobFinishedStatus) update;
            if (!finished.isCommittedWhenAllFilesAdded()) {
                return FINISHED;
            } else {
                return UNCOMMITTED;
            }
        } else {
            throw new IllegalArgumentException("Unrecognised status update type: " + update.getClass().getSimpleName());
        }
    }

    /**
     * Tracks the furthest status in a run or job. For runs, an in progress or finished run will supersede a failed
     * one. For updates in a run, a failure will supersede any other status.
     */
    private static class StatusTracker {
        private final boolean orderInJob;
        private IngestJobStatusType latestStatus;

        public static StatusTracker runStatuses() {
            return new StatusTracker(true);
        }

        public static StatusTracker updateStatuses() {
            return new StatusTracker(false);
        }

        private StatusTracker(boolean orderInJob) {
            this.orderInJob = orderInJob;
        }

        public void setIfLater(IngestJobStatusType newStatus) {
            if (latestStatus == null || order(latestStatus) < order(newStatus)) {
                latestStatus = newStatus;
            }
        }

        private int order(IngestJobStatusType statusType) {
            if (orderInJob) {
                return statusType.orderInJob;
            } else {
                return statusType.orderInRun;
            }
        }

        public IngestJobStatusType get() {
            return latestStatus;
        }
    }

}
