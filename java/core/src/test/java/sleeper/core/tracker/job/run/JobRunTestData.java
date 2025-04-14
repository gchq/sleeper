/*
 * Copyright 2022-2025 Crown Copyright
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
     * Creates a job run with the given status updates.
     *
     * @param  taskId        the task ID to set
     * @param  statusUpdates the status updates
     * @return               the run
     */
    public static JobRun jobRunOnTask(String taskId, JobStatusUpdate... statusUpdates) {
        return jobRun(JobRun.builder().taskId(taskId), statusUpdates);
    }

    /**
     * Creates a job run with the given status updates, that occurred on no task. This usually happens when validation
     * occurs outside of the task, but is still specific to the run, e.g. in the bulk import starter lambda.
     *
     * @param  statusUpdates the status updates
     * @return               the run
     */
    public static JobRun validationRun(JobStatusUpdate... statusUpdates) {
        return jobRun(JobRun.builder(), statusUpdates);
    }

    private static JobRun jobRun(JobRun.Builder builder, JobStatusUpdate... statusUpdates) {
        Stream.of(statusUpdates)
                .sorted(Comparator.comparing(JobStatusUpdate::getUpdateTime))
                .forEach(builder::statusUpdate);
        return builder.build();
    }

}
