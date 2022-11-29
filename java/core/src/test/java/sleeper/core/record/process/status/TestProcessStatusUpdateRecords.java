/*
 * Copyright 2022 Crown Copyright
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
import java.util.stream.Stream;

public class TestProcessStatusUpdateRecords {

    public static final String DEFAULT_JOB_ID = "test-job-id";
    public static final String DEFAULT_TASK_ID = "test-task-id";
    public static final String TASK_ID_1 = "task-id-1";
    public static final String TASK_ID_2 = "task-id-2";
    private static final Instant DEFAULT_EXPIRY = Instant.ofEpochSecond(999999999);
    private final List<ProcessStatusUpdateRecord> updates = new ArrayList<>();

    public static TestProcessStatusUpdateRecords records() {
        return new TestProcessStatusUpdateRecords();
    }

    public TestProcessStatusUpdateRecords fromUpdates(TaskUpdates... taskUpdates) {
        Stream.of(taskUpdates)
                .flatMap(task -> task.recordsWithExpiry(DEFAULT_EXPIRY))
                .forEach(updates::add);
        return this;
    }

    public TestProcessStatusUpdateRecords fromUpdates(ProcessStatusUpdate... statusUpdates) {
        return fromUpdates(forJobOnTask(DEFAULT_JOB_ID, DEFAULT_TASK_ID, statusUpdates));
    }

    public static TaskUpdates onTask(
            String taskId, ProcessStatusUpdate... updates) {
        return forJobOnTask(DEFAULT_JOB_ID, taskId, updates);
    }

    public static TaskUpdates forJobOnTask(
            String jobId, String taskId, ProcessStatusUpdate... updates) {
        return new TaskUpdates(jobId, taskId, Arrays.asList(updates));
    }

    public Stream<ProcessStatusUpdateRecord> stream() {
        return updates.stream();
    }

    public static class TaskUpdates {
        private final String jobId;
        private final String taskId;
        private final List<ProcessStatusUpdate> statusUpdates;

        public TaskUpdates(String jobId, String taskId, List<ProcessStatusUpdate> statusUpdates) {
            this.jobId = jobId;
            this.taskId = taskId;
            this.statusUpdates = statusUpdates;
        }

        public Stream<ProcessStatusUpdateRecord> recordsWithExpiry(Instant expiryDate) {
            return statusUpdates.stream()
                    .map(update -> new ProcessStatusUpdateRecord(jobId, expiryDate, update, taskId));
        }
    }
}
