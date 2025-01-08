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
package sleeper.core.tracker.job;

import sleeper.core.tracker.job.status.ProcessRun;
import sleeper.core.tracker.job.status.ProcessRunFinishedUpdate;
import sleeper.core.tracker.job.status.ProcessRunStartedUpdate;
import sleeper.core.tracker.job.status.ProcessStatusUpdate;

/**
 * A helper for creating runs for tests.
 */
public class ProcessRunTestData {

    private ProcessRunTestData() {
    }

    /**
     * Creates a run with a started status.
     *
     * @param  taskId        the task ID to set
     * @param  startedStatus the started status to set
     * @return               the run
     */
    public static ProcessRun startedRun(String taskId, ProcessRunStartedUpdate startedStatus) {
        return ProcessRun.builder()
                .taskId(taskId)
                .startedStatus(startedStatus)
                .build();
    }

    /**
     * Creates a run with a started status and a finished status.
     *
     * @param  taskId         the task ID to set
     * @param  startedStatus  the started status to set
     * @param  finishedStatus the finished status to set
     * @return                the run
     */
    public static ProcessRun finishedRun(String taskId, ProcessRunStartedUpdate startedStatus, ProcessRunFinishedUpdate finishedStatus) {
        return ProcessRun.builder()
                .taskId(taskId)
                .startedStatus(startedStatus)
                .finishedStatus(finishedStatus)
                .build();
    }

    /**
     * Creates a run with a validated status, a started status and a finished status.
     *
     * @param  taskId         the task ID to set
     * @param  startedStatus  the started status to set
     * @param  finishedStatus the finished status to set
     * @return                the run
     */
    public static ProcessRun validatedFinishedRun(String taskId, ProcessRunStartedUpdate validatedStatus, ProcessRunStartedUpdate startedStatus, ProcessRunFinishedUpdate finishedStatus) {
        return ProcessRun.builder()
                .taskId(taskId)
                .startedStatus(validatedStatus)
                .statusUpdate(startedStatus)
                .finishedStatus(finishedStatus)
                .build();
    }

    /**
     * Creates a run with a started status that occurred on no task.
     *
     * @param  validationStatus the started status to set
     * @return                  the run
     */
    public static ProcessRun validationRun(ProcessRunStartedUpdate validationStatus) {
        return ProcessRun.builder()
                .startedStatus(validationStatus)
                .build();
    }

    /**
     * Creates a run with a started status that occurred on no task.
     *
     * @param  startedStatus the started status to set
     * @param  updates       the other updates
     * @return               the run
     */
    public static ProcessRun unfinishedRun(String taskId, ProcessRunStartedUpdate startedStatus, ProcessStatusUpdate... updates) {
        ProcessRun.Builder builder = ProcessRun.builder().taskId(taskId).startedStatus(startedStatus);
        for (ProcessStatusUpdate update : updates) {
            builder.statusUpdate(update);
        }
        return builder.build();
    }

}
