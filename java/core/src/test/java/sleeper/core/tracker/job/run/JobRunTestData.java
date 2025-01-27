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
package sleeper.core.tracker.job.run;

import sleeper.core.tracker.job.status.JobRunStartedUpdate;
import sleeper.core.tracker.job.status.JobStatusUpdate;

import java.util.Comparator;
import java.util.stream.Stream;

/**
 * A helper for creating job runs for tests.
 */
public class JobRunTestData {

    private JobRunTestData() {
    }

    /**
     * Creates a run with a started status.
     *
     * @param  taskId        the task ID to set
     * @param  statusUpdates the status updates
     * @return               the run
     */
    public static JobRun runOnTask(String taskId, JobStatusUpdate... statusUpdates) {
        JobRun.Builder builder = JobRun.builder().taskId(taskId);
        Stream.of(statusUpdates)
                .sorted(Comparator.comparing(JobStatusUpdate::getUpdateTime))
                .forEach(builder::statusUpdateDetectType);
        return builder.build();
    }

    /**
     * Creates a run with a started status that occurred on no task.
     *
     * @param  validationStatus the started status to set
     * @return                  the run
     */
    public static JobRun validationRun(JobRunStartedUpdate validationStatus) {
        return JobRun.builder()
                .startedStatus(validationStatus)
                .build();
    }

}
