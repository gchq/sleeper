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
import java.util.List;
import java.util.Objects;

public class ProcessRun {
    private final String taskId;
    private final List<ProcessStatusUpdate> statusUpdates = new ArrayList<>();

    private ProcessRun(Builder builder) {
        taskId = Objects.requireNonNull(builder.taskId, "taskId must not be null");
        statusUpdates.addAll(builder.statusUpdates);
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
        return (ProcessRunStartedUpdate) statusUpdates.stream()
                .filter(update -> update instanceof ProcessRunStartedUpdate)
                .findFirst().orElse(null);
    }

    public ProcessFinishedStatus getFinishedStatus() {
        return (ProcessFinishedStatus) statusUpdates.stream()
                .filter(update -> update instanceof ProcessFinishedStatus)
                .findFirst().orElse(null);
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

    public RecordsProcessedSummary getFinishedSummary() {
        if (isFinished()) {
            return getFinishedStatus().getSummary();
        } else {
            return null;
        }
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
        private final List<ProcessStatusUpdate> statusUpdates = new ArrayList<>();

        private Builder() {
        }

        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder startedStatus(ProcessRunStartedUpdate startedStatus) {
            this.statusUpdates.add(startedStatus);
            return this;
        }

        public Builder finishedStatus(ProcessFinishedStatus finishedStatus) {
            this.statusUpdates.add(finishedStatus);
            return this;
        }

        public Builder statusUpdates(ProcessStatusUpdate... statusUpdates) {
            return statusUpdates(List.of(statusUpdates));
        }

        public Builder statusUpdates(List<ProcessStatusUpdate> statusUpdates) {
            this.statusUpdates.addAll(statusUpdates);
            return this;
        }

        public ProcessRun build() {
            return new ProcessRun(this);
        }
    }
}
