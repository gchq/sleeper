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
import java.util.Objects;

public class ProcessStatusUpdateRecord {

    private final String jobId;
    private final Instant expiryDate;
    private final ProcessStatusUpdate statusUpdate;
    private final String runId;
    private final String taskId;

    public ProcessStatusUpdateRecord(String jobId, Instant expiryDate, ProcessStatusUpdate statusUpdate, String taskId) {
        this(jobId, expiryDate, statusUpdate, null, taskId);
    }

    public ProcessStatusUpdateRecord(String jobId, Instant expiryDate, ProcessStatusUpdate statusUpdate, String runId, String taskId) {
        this.jobId = Objects.requireNonNull(jobId, "jobId must not be null");
        this.expiryDate = expiryDate;
        this.statusUpdate = Objects.requireNonNull(statusUpdate, "statusUpdate must not be null");
        this.runId = runId;
        this.taskId = taskId;
    }

    public String getJobId() {
        return jobId;
    }

    public Instant getExpiryDate() {
        return expiryDate;
    }

    public ProcessStatusUpdate getStatusUpdate() {
        return statusUpdate;
    }

    public Instant getUpdateTime() {
        return statusUpdate.getUpdateTime();
    }

    public String getRunId() {
        return runId;
    }

    public String getTaskId() {
        return taskId;
    }
}
