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

import sleeper.core.tracker.job.status.JobRunEndUpdate;
import sleeper.core.tracker.job.status.JobRunStartedUpdate;
import sleeper.core.tracker.job.status.JobStatusUpdate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Information about a single run of a job that was tracked.
 */
public class JobRun implements JobRunReport {
    private final String taskId;
    private final List<JobStatusUpdate> statusUpdates;
    private final JobRunStartedUpdate startedStatus;
    private final JobRunEndUpdate finishedStatus;
    private final Instant summaryStartTime;

    private JobRun(Builder builder) {
        taskId = builder.taskId;
        statusUpdates = Collections.unmodifiableList(builder.statusUpdates);
        startedStatus = builder.startedStatus;
        finishedStatus = builder.finishedStatus;
        summaryStartTime = builder.summaryStartTime;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String getTaskId() {
        return taskId;
    }

    @Override
    public boolean isFinished() {
        return finishedStatus != null;
    }

    @Override
    public boolean isFinishedSuccessfully() {
        return finishedStatus != null && finishedStatus.isSuccessful();
    }

    @Override
    public Instant getStartTime() {
        return Optional.ofNullable(startedStatus).map(JobRunStartedUpdate::getStartTime).orElse(null);
    }

    @Override
    public Instant getFinishTime() {
        if (isFinished()) {
            return finishedStatus.getFinishTime();
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
        return getLatestUpdate().getUpdateTime();
    }

    public JobStatusUpdate getFirstUpdate() {
        return statusUpdates.get(0);
    }

    public JobStatusUpdate getLatestUpdate() {
        return statusUpdates.get(statusUpdates.size() - 1);
    }

    @Override
    public JobRunSummary getFinishedSummary() {
        if (isFinished()) {
            Instant startTime = Optional.ofNullable(summaryStartTime)
                    .orElseGet(finishedStatus::getFinishTime);
            Instant finishTime = finishedStatus.getFinishTime();
            JobRunTime runTime = finishedStatus.getTimeInProcess()
                    .map(timeInProcess -> new JobRunTime(startTime, finishTime, timeInProcess))
                    .orElseGet(() -> new JobRunTime(startTime, finishTime));
            return new JobRunSummary(finishedStatus.getRecordsProcessed(), runTime);
        } else {
            return null;
        }
    }

    public List<JobStatusUpdate> getStatusUpdates() {
        return statusUpdates;
    }

    /**
     * Gets the last status update of the provided type.
     *
     * @param  <T>        the status update type
     * @param  updateType the class defining the type of update to look for
     * @return            the last status update, or an empty optional if there is no update of the given type
     */
    public <T extends JobStatusUpdate> Optional<T> getLastStatusOfType(Class<T> updateType) {
        for (int i = statusUpdates.size() - 1; i >= 0; i--) {
            if (updateType.isInstance(statusUpdates.get(i))) {
                return Optional.of(updateType.cast(statusUpdates.get(i)));
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
        JobRun that = (JobRun) o;
        return Objects.equals(taskId, that.taskId) && Objects.equals(statusUpdates, that.statusUpdates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, statusUpdates);
    }

    @Override
    public String toString() {
        return "JobRun{" +
                "taskId='" + taskId + '\'' +
                ", statusUpdates=" + statusUpdates +
                '}';
    }

    /**
     * Creates a process run object.
     */
    public static final class Builder {
        private String taskId;
        private final List<JobStatusUpdate> statusUpdates = new ArrayList<>();
        private JobRunStartedUpdate startedStatus;
        private JobRunEndUpdate finishedStatus;
        private Instant summaryStartTime;

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
         * Adds a status update.
         *
         * @param  statusUpdate the status update to add
         * @return              the builder
         */
        public Builder statusUpdate(JobStatusUpdate statusUpdate) {
            if (statusUpdate instanceof JobRunStartedUpdate) {
                JobRunStartedUpdate startedStatus = (JobRunStartedUpdate) statusUpdate;
                if (startedStatus.isStartOfRun()) {
                    this.startedStatus = startedStatus;
                }
                if (startedStatus.isTimeForRunSummary()) {
                    this.summaryStartTime = startedStatus.getStartTime();
                }
            }
            if (statusUpdate instanceof JobRunEndUpdate) {
                this.finishedStatus = (JobRunEndUpdate) statusUpdate;
            }
            this.statusUpdates.add(statusUpdate);
            return this;
        }

        public JobRun build() {
            return new JobRun(this);
        }
    }
}
