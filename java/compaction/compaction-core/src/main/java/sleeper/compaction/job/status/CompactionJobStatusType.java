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

import sleeper.core.record.process.status.ProcessRun;

import java.util.Collection;

/**
 * Defines the states a compaction job can be in. Uses an order to find which run of the job determines the state of the
 * job as a whole, as a job can be run multiple times. If there is a run of the job which is in progress or successful,
 * any failed runs will be ignored for computing the status of the job.
 */
public enum CompactionJobStatusType {
    PENDING(1),
    FAILED(2),
    IN_PROGRESS(3),
    FINISHED(4);

    private final int order;

    CompactionJobStatusType(int order) {
        this.order = order;
    }

    /**
     * Gets the furthest status type for any run of a compaction job.
     *
     * @param  runStatusTypes the status types of runs in the job
     * @return                the status type
     */
    public static CompactionJobStatusType statusTypeOfFurthestRunOfJob(Collection<CompactionJobStatusType> runStatusTypes) {
        FurthestStatusTracker furthestStatus = new FurthestStatusTracker();
        for (CompactionJobStatusType runStatusType : runStatusTypes) {
            furthestStatus.setIfFurther(runStatusType);
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
