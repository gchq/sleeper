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

package sleeper.core.record.process.status;

import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Stores information about a job run.
 */
public class ProcessRun {
    private final String taskId;
    private final ProcessRunStartedUpdate startedStatus;
    private final ProcessRunFinishedUpdate finishedStatus;
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

    /**
     * Creates an instance of this class with a started status.
     *
     * @param  taskId        the task ID to set
     * @param  startedStatus the started status to set
     * @return               an instance of this class
     */
    public static ProcessRun started(String taskId, ProcessRunStartedUpdate startedStatus) {
        return builder().taskId(taskId)
                .startedStatus(startedStatus)
                .build();
    }

    /**
     * Creates an instance of this class with a started status and a finished status.
     *
     * @param  taskId         the task ID to set
     * @param  startedStatus  the started status to set
     * @param  finishedStatus the finished status to set
     * @return                an instance of this class
     */
    public static ProcessRun finished(String taskId, ProcessRunStartedUpdate startedStatus, ProcessRunFinishedUpdate finishedStatus) {
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

    public ProcessRunFinishedUpdate getFinishedStatus() {
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

    /**
     * Gets the finish time for this run.
     *
     * @return the finish time
     */
    public Instant getFinishTime() {
        if (isFinished()) {
            return getFinishedStatus().getSummary().getFinishTime();
        } else {
            return null;
        }
    }

    /**
     * Gets the finish update time for this run.
     *
     * @return the finish update time
     */
    public Instant getFinishUpdateTime() {
        if (isFinished()) {
            return getFinishedStatus().getUpdateTime();
        } else {
            return null;
        }
    }

    /**
     * Gets the latest update time for this run.
     *
     * @return the latest update time
     */
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

    /**
     * Gets the finished summary for this run.
     *
     * @return the finished summary
     */
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

    /**
     * Gets the last status update of the provided type.
     *
     * @param  <T> the type of status update to look for
     * @param  cls the class to get the type from
     * @return     the last status update casted to {@link T}
     */
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

    /**
     * Builder class for creating a process run.
     */
    public static final class Builder {
        private String taskId;
        private ProcessRunStartedUpdate startedStatus;
        private ProcessRunFinishedUpdate finishedStatus;
        private final List<ProcessStatusUpdate> statusUpdates = new ArrayList<>();

        private Builder() {
        }

        /**
         * Sets the task ID.
         *
         * @param  taskId the task ID to set
         * @return        the builder
         */
        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        /**
         * Adds the started status.
         *
         * @param  startedStatus the started status to set
         * @return               the builder
         */
        public Builder startedStatus(ProcessRunStartedUpdate startedStatus) {
            this.startedStatus = startedStatus;
            if (startedStatus instanceof ProcessRunFinishedUpdate) {
                this.finishedStatus = (ProcessRunFinishedUpdate) startedStatus;
            }
            this.statusUpdates.add(startedStatus);
            return this;
        }

        /**
         * Adds the finished status.
         *
         * @param  finishedStatus the finished status to set
         * @return                the builder
         */
        public Builder finishedStatus(ProcessRunFinishedUpdate finishedStatus) {
            this.finishedStatus = finishedStatus;
            this.statusUpdates.add(finishedStatus);
            return this;
        }

        /**
         * Adds the status update.
         *
         * @param  statusUpdate the status update to set
         * @return              the builder
         */
        public Builder statusUpdate(ProcessStatusUpdate statusUpdate) {
            this.statusUpdates.add(statusUpdate);
            return this;
        }

        public ProcessRun build() {
            return new ProcessRun(this);
        }
    }
}
