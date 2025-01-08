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

package sleeper.core.tracker.job.status;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A test helper to simulate the contents of a job tracker. This will specify status updates for jobs running on tasks,
 * and create a list of records that would appear in the job tracker for those updates. This can be used to test any
 * code that requires records from the job tracker.
 */
public class TestProcessStatusUpdateRecords {

    public static final String DEFAULT_JOB_ID = "test-job-id";
    public static final String DEFAULT_TASK_ID = "test-task-id";
    public static final String TASK_ID_1 = "task-id-1";
    public static final String TASK_ID_2 = "task-id-2";
    public static final Instant DEFAULT_EXPIRY = Instant.ofEpochSecond(999999999);
    private final List<JobStatusUpdateRecord> updates = new ArrayList<>();

    /**
     * Creates an instance of this class with no process status updates.
     *
     * @return an instance of this class with no process status updates
     */
    public static TestProcessStatusUpdateRecords records() {
        return new TestProcessStatusUpdateRecords();
    }

    /**
     * Adds the process status updates for the provided task updates to this class.
     *
     * @param  taskUpdates a collection of {@link TaskUpdates}
     * @return             this instance for chaining
     */
    public TestProcessStatusUpdateRecords fromUpdates(TaskUpdates... taskUpdates) {
        Stream.of(taskUpdates)
                .flatMap(TaskUpdates::records)
                .forEach(updates::add);
        return this;
    }

    /**
     * Adds the process status updates to this class.
     *
     * @param  statusUpdates a collection of {@link JobStatusUpdate}
     * @return               this instance for chaining
     */
    public TestProcessStatusUpdateRecords fromUpdates(JobStatusUpdate... statusUpdates) {
        return fromUpdates(forJobOnTask(DEFAULT_JOB_ID, DEFAULT_TASK_ID, statusUpdates));
    }

    /**
     * Creates an instance of task updates.
     *
     * @param  jobId   the job ID
     * @param  taskId  the task ID
     * @param  updates the process status updates
     * @return         a {@link TaskUpdates} instance
     */
    public static TaskUpdates forJobOnTask(
            String jobId, String taskId, JobStatusUpdate... updates) {
        return forJobRunOnTask(jobId, null, taskId, updates);
    }

    /**
     * Creates an instance of task updates.
     *
     * @param  jobId    the job ID
     * @param  jobRunId the job run ID
     * @param  taskId   the task ID
     * @param  updates  the process status updates
     * @return          a {@link TaskUpdates} instance
     */
    public static TaskUpdates forJobRunOnTask(
            String jobId, String jobRunId, String taskId, JobStatusUpdate... updates) {
        return new TaskUpdates(jobId, jobRunId, taskId,
                Stream.of(updates)
                        .map(update -> new UpdateWithExpiry(update, DEFAULT_EXPIRY))
                        .collect(Collectors.toList()));
    }

    private static TaskUpdates forJobOnTask(
            String jobId, String taskId, UpdateWithExpiry... updates) {
        return new TaskUpdates(jobId, taskId, List.of(updates));
    }

    /**
     * Creates an instance of task updates with the default task ID.
     *
     * @param  jobId   the job ID
     * @param  updates the process status updates
     * @return         a {@link TaskUpdates} instance
     */
    public static TaskUpdates forJob(
            String jobId, JobStatusUpdate... updates) {
        return forJobOnTask(jobId, DEFAULT_TASK_ID, updates);
    }

    /**
     * Creates an instance of task updates with the default task ID.
     *
     * @param  jobId   the job ID
     * @param  updates the {@link UpdateWithExpiry} objects
     * @return         a {@link TaskUpdates} instance
     */
    public static TaskUpdates forJob(
            String jobId, UpdateWithExpiry... updates) {
        return forJobOnTask(jobId, DEFAULT_TASK_ID, updates);
    }

    /**
     * Creates an instance of task updates with the default job ID.
     *
     * @param  taskId  the task ID
     * @param  updates the process status updates
     * @return         a {@link TaskUpdates} instance
     */
    public static TaskUpdates onTask(
            String taskId, JobStatusUpdate... updates) {
        return forJobOnTask(DEFAULT_JOB_ID, taskId, updates);
    }

    /**
     * Creates an instance of task updates with the default job ID and no task ID.
     *
     * @param  updates the process status updates
     * @return         a {@link TaskUpdates} instance
     */
    public static TaskUpdates onNoTask(JobStatusUpdate... updates) {
        return forJobOnTask(DEFAULT_JOB_ID, null, updates);
    }

    /**
     * Creates an instance of task updates with the default job ID.
     *
     * @param  jobRunId the job run ID
     * @param  taskId   the task ID
     * @param  updates  the process status updates
     * @return          a {@link TaskUpdates} instance
     */
    public static TaskUpdates forRunOnTask(String jobRunId, String taskId, JobStatusUpdate... updates) {
        return forJobRunOnTask(DEFAULT_JOB_ID, jobRunId, taskId, updates);
    }

    /**
     * Creates an instance of task updates with the default job ID and no task ID.
     *
     * @param  jobRunId the job run ID
     * @param  updates  the process status updates
     * @return          a {@link TaskUpdates} instance
     */
    public static TaskUpdates forRunOnNoTask(String jobRunId, JobStatusUpdate... updates) {
        return forJobRunOnTask(DEFAULT_JOB_ID, jobRunId, null, updates);
    }

    /**
     * Creates an instance of task updates with no task ID.
     *
     * @param  jobId    the job ID
     * @param  jobRunId the job run ID
     * @param  updates  the process status updates
     * @return          a {@link TaskUpdates} instance
     */
    public static TaskUpdates forJobRunOnNoTask(String jobId, String jobRunId, JobStatusUpdate... updates) {
        return forJobRunOnTask(jobId, jobRunId, null, updates);
    }

    /**
     * Creates an instance of task updates with no run ID and no task ID.
     *
     * @param  jobId   the job ID
     * @param  updates the process status updates
     * @return         a {@link TaskUpdates} instance
     */
    public static TaskUpdates forNoRunNoTask(String jobId, JobStatusUpdate... updates) {
        return forJobRunOnTask(jobId, null, null, updates);
    }

    /**
     * Creates an update with expiry instance.
     *
     * @param  expiryTime the expiry time
     * @param  update     the process status update
     * @return            an instance of {@link UpdateWithExpiry}
     */
    public static UpdateWithExpiry withExpiry(Instant expiryTime, JobStatusUpdate update) {
        return new UpdateWithExpiry(update, expiryTime);
    }

    /**
     * Streams the process status update records.
     *
     * @return a stream of {@link JobStatusUpdateRecord}
     */
    public Stream<JobStatusUpdateRecord> stream() {
        return updates.stream();
    }

    /**
     * A class representing a running task with updates.
     */
    public static class TaskUpdates {
        private final String jobId;
        private final String jobRunId;
        private final String taskId;
        private final List<UpdateWithExpiry> statusUpdates;

        private TaskUpdates(String jobId, String taskId, List<UpdateWithExpiry> statusUpdates) {
            this(jobId, null, taskId, statusUpdates);
        }

        private TaskUpdates(String jobId, String jobRunId, String taskId, List<UpdateWithExpiry> statusUpdates) {
            this.jobId = jobId;
            this.jobRunId = jobRunId;
            this.taskId = taskId;
            this.statusUpdates = statusUpdates;
        }

        /**
         * Creates a stream of process update records.
         *
         * @return a stream of process update records
         */
        public Stream<JobStatusUpdateRecord> records() {
            return statusUpdates.stream()
                    .map(update -> JobStatusUpdateRecord.builder()
                            .jobId(jobId).statusUpdate(update.statusUpdate)
                            .jobRunId(jobRunId).taskId(taskId).expiryDate(update.expiryTime)
                            .build());
        }
    }

    /**
     * A class representing a process status update with an expiry time.
     */
    public static class UpdateWithExpiry {
        private final JobStatusUpdate statusUpdate;
        private final Instant expiryTime;

        private UpdateWithExpiry(JobStatusUpdate statusUpdate, Instant expiryTime) {
            this.statusUpdate = statusUpdate;
            this.expiryTime = expiryTime;
        }
    }
}
