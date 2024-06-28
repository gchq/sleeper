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
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessStatusUpdate;

import java.util.stream.Stream;

/**
 * Defines the types of updates during a compaction job. Can also find the furthest update in a run of a compaction job,
 * where a job may be run multiple times. Uses an order to find which update is the furthest, where a failure supersedes
 * any other update.
 */
public enum CompactionJobUpdateType {
    CREATED(1, CompactionJobCreatedStatus.class, CompactionJobStatusType.PENDING),
    STARTED(2, CompactionJobStartedStatus.class, CompactionJobStatusType.IN_PROGRESS),
    FINISHED(3, CompactionJobFinishedStatus.class, CompactionJobStatusType.FINISHED),
    COMMITTED(4, CompactionJobCommittedStatus.class, CompactionJobStatusType.FINISHED),
    FAILED(5, ProcessFailedStatus.class, CompactionJobStatusType.FAILED);

    private final int order;
    private final Class<?> statusUpdateClass;
    private final CompactionJobStatusType jobStatusTypeAfterUpdate;

    CompactionJobUpdateType(int order, Class<?> statusUpdateClass, CompactionJobStatusType jobStatusTypeAfterUpdate) {
        this.order = order;
        this.statusUpdateClass = statusUpdateClass;
        this.jobStatusTypeAfterUpdate = jobStatusTypeAfterUpdate;
    }

    /**
     * Gets the furthest update type for a run of a compaction job.
     *
     * @param  run the run
     * @return     the update type
     */
    public static CompactionJobUpdateType typeOfFurthestUpdateInRun(ProcessRun run) {
        FurthestUpdateTracker furthestUpdate = new FurthestUpdateTracker();
        for (ProcessStatusUpdate update : run.getStatusUpdates()) {
            furthestUpdate.setIfFurther(typeOfUpdate(update));
        }
        return furthestUpdate.get();
    }

    public static CompactionJobUpdateType typeOfUpdate(ProcessStatusUpdate update) {
        return Stream.of(values())
                .filter(type -> type.statusUpdateClass.isInstance(update))
                .findFirst().orElseThrow();
    }

    public CompactionJobStatusType getJobStatusTypeAfterUpdate() {
        return jobStatusTypeAfterUpdate;
    }

    /**
     * Tracks the furthest update in a run. A failure will supersede any other update.
     */
    private static class FurthestUpdateTracker {
        private CompactionJobUpdateType furthestType;

        public void setIfFurther(CompactionJobUpdateType newType) {
            if (furthestType == null || furthestType.order < newType.order) {
                furthestType = newType;
            }
        }

        public CompactionJobUpdateType get() {
            return furthestType;
        }
    }
}
