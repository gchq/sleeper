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

import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class ProcessRun {
    private final String taskId;
    private final ProcessRunStartedUpdate startedStatus;
    private final ProcessFinishedStatus finishedStatus;
    private final List<ProcessStatusUpdate> statusUpdates;

    private ProcessRun(Builder builder) {
        taskId = builder.taskId;
        startedStatus = Objects.requireNonNull(builder.startedStatus, "startedStatus must not be null");
        finishedStatus = builder.finishedStatus;
        statusUpdates = Collections.unmodifiableList(builder.statusUpdates);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static ProcessRun started(String taskId, ProcessRunStartedUpdate startedStatus) {
        return builder().taskId(taskId)
                .startedStatus(startedStatus)
                .build();
    }

    public static ProcessRun finished(String taskId, ProcessRunStartedUpdate startedStatus, ProcessFinishedStatus finishedStatus) {
        return builder().taskId(taskId)
                .startedStatus(startedStatus)
                .finishedStatus(finishedStatus)
                .build();
    }

    public String getTaskId() {
        return taskId;
    }

    public ProcessRunStartedUpdate getStartedStatus() {
        return startedStatus;
    }

    public ProcessFinishedStatus getFinishedStatus() {
        return finishedStatus;
    }

    public boolean isFinished() {
        return getFinishedStatus() != null;
    }

    public Instant getStartTime() {
        return getStartedStatus().getStartTime();
    }

    public Instant getStartUpdateTime() {
        return getStartedStatus().getUpdateTime();
    }

    public Instant getFinishTime() {
        if (isFinished()) {
            return getFinishedStatus().getSummary().getFinishTime();
        } else {
            return null;
        }
    }

    public Instant getFinishUpdateTime() {
        if (isFinished()) {
            return getFinishedStatus().getUpdateTime();
        } else {
            return null;
        }
    }

    public Instant getLatestUpdateTime() {
        if (isFinished()) {
            return getFinishedStatus().getUpdateTime();
        } else {
            return getStartUpdateTime();
        }
    }

    public ProcessStatusUpdate getLatestUpdate() {
        return statusUpdates.get(statusUpdates.size() - 1);
    }

    public RecordsProcessedSummary getFinishedSummary() {
        if (isFinished()) {
            return getFinishedStatus().getSummary();
        } else {
            return null;
        }
    }

    public List<ProcessStatusUpdate> getStatusUpdates() {
        return statusUpdates;
    }

    public <T extends ProcessStatusUpdate> Optional<T> getLastStatusOfType(Class<T> cls) {
        for (int i = statusUpdates.size() - 1; i >= 0; i--) {
            if (cls.isInstance(statusUpdates.get(i))) {
                return Optional.of(cls.cast(statusUpdates.get(i)));
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProcessRun that = (ProcessRun) o;
        return Objects.equals(taskId, that.taskId) && Objects.equals(statusUpdates, that.statusUpdates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, statusUpdates);
    }

    @Override
    public String toString() {
        return "ProcessRun{" +
                "taskId='" + taskId + '\'' +
                ", statusUpdates=" + statusUpdates +
                '}';
    }

    public static final class Builder {
        private String taskId;
        private ProcessRunStartedUpdate startedStatus;
        private ProcessFinishedStatus finishedStatus;
        private final List<ProcessStatusUpdate> statusUpdates = new ArrayList<>();

        private Builder() {
        }

        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder startedStatus(ProcessRunStartedUpdate startedStatus) {
            this.startedStatus = startedStatus;
            this.statusUpdates.add(startedStatus);
            return this;
        }

        public Builder finishedStatus(ProcessFinishedStatus finishedStatus) {
            this.finishedStatus = finishedStatus;
            this.statusUpdates.add(finishedStatus);
            return this;
        }

        public Builder statusUpdate(ProcessStatusUpdate statusUpdate) {
            this.statusUpdates.add(statusUpdate);
            return this;
        }

        public ProcessRun build() {
            return new ProcessRun(this);
        }
    }
}
