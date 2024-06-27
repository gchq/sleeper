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
package sleeper.compaction.job.status;

import sleeper.core.record.process.status.ProcessFailedStatus;
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessRun;

/**
 * Defines the states a compaction job can be in. Uses an order to find which run of the job determines the state of the
 * job as a whole, as a job can be run multiple times. If there is a run of the job which is in progress or successful,
 * any failed runs will be ignored for computing the status of the job.
 */
public enum CompactionJobStatusType {
    PENDING(CompactionJobCreatedStatus.class, 1),
    FAILED(ProcessFailedStatus.class, 2),
    IN_PROGRESS(CompactionJobStartedStatus.class, 3),
    FINISHED(ProcessFinishedStatus.class, 4);

    private final Class<?> statusUpdateClass;
    private final int order;

    CompactionJobStatusType(Class<?> statusUpdateClass, int order) {
        this.statusUpdateClass = statusUpdateClass;
        this.order = order;
    }

    /**
     * Gets the furthest status type for any run of a compaction job.
     *
     * @param  job the job
     * @return     the status type
     */
    public static CompactionJobStatusType statusTypeOfFurthestRunOfJob(CompactionJobStatus job) {
        FurthestStatusTracker furthestStatus = new FurthestStatusTracker();
        for (ProcessRun run : job.getJobRuns()) {
            furthestStatus.setIfFurther(statusTypeOfJobRun(run));
        }
        return furthestStatus.get();
    }

    /**
     * Gets the status type for a run of a compaction job.
     *
     * @param  run the run
     * @return     the status type
     */
    public static CompactionJobStatusType statusTypeOfJobRun(ProcessRun run) {
        return CompactionJobUpdateType.typeOfFurthestUpdateInRun(run).getJobStatusTypeAfterUpdate();
    }

    /**
     * Tracks the furthest status in a job. An in progress or finished run will supersede a failed one.
     */
    private static class FurthestStatusTracker {
        private CompactionJobStatusType furthestStatus = PENDING;

        public void setIfFurther(CompactionJobStatusType newStatus) {
            if (furthestStatus == null || furthestStatus.order < newStatus.order) {
                furthestStatus = newStatus;
            }
        }

        public CompactionJobStatusType get() {
            return furthestStatus;
        }
    }
}
