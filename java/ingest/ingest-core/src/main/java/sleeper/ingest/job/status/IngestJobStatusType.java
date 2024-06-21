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

import sleeper.core.record.process.status.ProcessRun;

/**
 * Defines the states an ingest job can be in. Uses an order to find which run of the job determines the state of the
 * job as a whole, as a job can be run multiple times. If there is a run of the job which is in progress or successful,
 * any failed runs will be ignored for computing the status of the job.
 */
public enum IngestJobStatusType {
    REJECTED(1),
    ACCEPTED(2),
    FAILED(3),
    IN_PROGRESS(4),
    UNCOMMITTED(5),
    FINISHED(6);

    private final int order;

    IngestJobStatusType(int order) {
        this.order = order;
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
        StatusTracker furthestStatus = new StatusTracker();
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
        return IngestJobUpdateType.typeOfFurthestUpdateInRun(run)
                .statusTypeAfterThisInRun(run);
    }

    /**
     * Tracks the furthest status in a job. An in progress or finished run will supersede a failed one.
     */
    private static class StatusTracker {
        private IngestJobStatusType latestStatus;

        public void setIfLater(IngestJobStatusType newStatus) {
            if (latestStatus == null || latestStatus.order < newStatus.order) {
                latestStatus = newStatus;
            }
        }

        public IngestJobStatusType get() {
            return latestStatus;
        }
    }

}
