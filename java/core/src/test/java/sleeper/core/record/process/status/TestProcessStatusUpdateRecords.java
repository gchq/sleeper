/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.core.record.process.status;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestProcessStatusUpdateRecords {

    public static final String DEFAULT_JOB_ID = "test-job-id";
    public static final String DEFAULT_TASK_ID = "test-task-id";
    public static final String TASK_ID_1 = "task-id-1";
    public static final String TASK_ID_2 = "task-id-2";
    public static final Instant DEFAULT_EXPIRY = Instant.ofEpochSecond(999999999);
    private final List<ProcessStatusUpdateRecord> updates = new ArrayList<>();

    public static TestProcessStatusUpdateRecords records() {
        return new TestProcessStatusUpdateRecords();
    }

    public TestProcessStatusUpdateRecords fromUpdates(TaskUpdates... taskUpdates) {
        Stream.of(taskUpdates)
                .flatMap(TaskUpdates::records)
                .forEach(updates::add);
        return this;
    }

    public TestProcessStatusUpdateRecords fromUpdates(ProcessStatusUpdate... statusUpdates) {
        return fromUpdates(forJobOnTask(DEFAULT_JOB_ID, DEFAULT_TASK_ID, statusUpdates));
    }

    public static TaskUpdates forJobOnTask(
            String jobId, String taskId, ProcessStatusUpdate... updates) {
        return forJobRunOnTask(jobId, null, taskId, updates);
    }

    public static TaskUpdates forJobRunOnTask(
            String jobId, String jobRunId, String taskId, ProcessStatusUpdate... updates) {
        return new TaskUpdates(jobId, jobRunId, taskId,
                Stream.of(updates)
                        .map(update -> new UpdateWithExpiry(update, DEFAULT_EXPIRY))
                        .collect(Collectors.toList()));
    }

    public static TaskUpdates forJobOnTask(
            String jobId, String taskId, UpdateWithExpiry... updates) {
        return new TaskUpdates(jobId, taskId, Arrays.asList(updates));
    }

    public static TaskUpdates forJob(
            String jobId, ProcessStatusUpdate... updates) {
        return forJobOnTask(jobId, DEFAULT_TASK_ID, updates);
    }

    public static TaskUpdates forJob(
            String jobId, UpdateWithExpiry... updates) {
        return forJobOnTask(jobId, DEFAULT_TASK_ID, updates);
    }

    public static TaskUpdates onTask(
            String taskId, ProcessStatusUpdate... updates) {
        return forJobOnTask(DEFAULT_JOB_ID, taskId, updates);
    }

    public static TaskUpdates onNoTask(ProcessStatusUpdate... updates) {
        return forJobOnTask(DEFAULT_JOB_ID, null, updates);
    }

    public static TaskUpdates forRunOnTask(String jobRunId, String taskId, ProcessStatusUpdate... updates) {
        return forJobRunOnTask(DEFAULT_JOB_ID, jobRunId, taskId, updates);
    }

    public static TaskUpdates forRunOnNoTask(String jobRunId, ProcessStatusUpdate... updates) {
        return forJobRunOnTask(DEFAULT_JOB_ID, jobRunId, null, updates);
    }

    public static TaskUpdates forNoRunNoTask(ProcessStatusUpdate... updates) {
        return forJobRunOnTask(DEFAULT_JOB_ID, null, null, updates);
    }

    public static UpdateWithExpiry withExpiry(Instant expiryTime, ProcessStatusUpdate update) {
        return new UpdateWithExpiry(update, expiryTime);
    }

    public Stream<ProcessStatusUpdateRecord> stream() {
        return updates.stream();
    }

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

        public Stream<ProcessStatusUpdateRecord> records() {
            return statusUpdates.stream()
                    .map(update -> ProcessStatusUpdateRecord.builder()
                            .jobId(jobId).statusUpdate(update.statusUpdate)
                            .jobRunId(jobRunId).taskId(taskId).expiryDate(update.expiryTime)
                            .build());
        }
    }

    public static class UpdateWithExpiry {
        private final ProcessStatusUpdate statusUpdate;
        private final Instant expiryTime;

        private UpdateWithExpiry(ProcessStatusUpdate statusUpdate, Instant expiryTime) {
            this.statusUpdate = statusUpdate;
            this.expiryTime = expiryTime;
        }
    }
}
