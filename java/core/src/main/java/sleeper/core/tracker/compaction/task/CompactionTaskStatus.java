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

package sleeper.core.tracker.compaction.task;

import sleeper.core.tracker.job.status.JobRunFinishedStatus;
import sleeper.core.tracker.job.status.ProcessRun;
import sleeper.core.tracker.job.status.TimeWindowQuery;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public class CompactionTaskStatus {
    private final String taskId;
    private final Instant startTime;
    private final CompactionTaskFinishedStatus finishedStatus;
    private final Instant expiryDate; // Set by database (null before status is saved)

    private CompactionTaskStatus(Builder builder) {
        taskId = Objects.requireNonNull(builder.taskId, "taskId must not be null");
        startTime = Objects.requireNonNull(builder.startTime, "startTime must not be null");
        finishedStatus = builder.finishedStatus;
        expiryDate = builder.expiryDate;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getTaskId() {
        return taskId;
    }

    public CompactionTaskFinishedStatus getFinishedStatus() {
        return finishedStatus;
    }

    public boolean isInPeriod(Instant windowStartTime, Instant windowEndTime) {
        TimeWindowQuery timeWindowQuery = new TimeWindowQuery(windowStartTime, windowEndTime);
        if (isFinished()) {
            return timeWindowQuery.isFinishedProcessInWindow(startTime, finishedStatus.getFinishTime());
        } else {
            return timeWindowQuery.isUnfinishedProcessInWindow(startTime);
        }
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getFinishTime() {
        if (isFinished()) {
            return finishedStatus.getFinishTime();
        } else {
            return null;
        }
    }

    public Duration getDuration() {
        if (isFinished()) {
            return Duration.between(startTime, finishedStatus.getFinishTime());
        } else {
            return null;
        }
    }

    public Instant getExpiryDate() {
        return expiryDate;
    }

    public boolean isFinished() {
        return finishedStatus != null;
    }

    public Integer getJobRunsOrNull() {
        if (isFinished()) {
            return finishedStatus.getTotalJobRuns();
        } else {
            return null;
        }
    }

    public int getJobRuns() {
        if (isFinished()) {
            return finishedStatus.getTotalJobRuns();
        } else {
            return 0;
        }
    }

    public ProcessRun asProcessRun() {
        return ProcessRun.builder().taskId(taskId)
                .startedStatus(CompactionTaskStartedStatus.startTime(getStartTime()))
                .finishedStatus(asProcessFinishedStatus())
                .build();
    }

    private JobRunFinishedStatus asProcessFinishedStatus() {
        if (finishedStatus == null) {
            return null;
        }
        return JobRunFinishedStatus.updateTimeAndSummary(
                finishedStatus.getFinishTime(),
                finishedStatus.asSummary(getStartTime()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactionTaskStatus that = (CompactionTaskStatus) o;
        return taskId.equals(that.taskId)
                && startTime.equals(that.startTime)
                && Objects.equals(finishedStatus, that.finishedStatus)
                && Objects.equals(expiryDate, that.expiryDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, startTime, finishedStatus, expiryDate);
    }

    @Override
    public String toString() {
        return "CompactionTaskStatus{" +
                "taskId='" + taskId + '\'' +
                ", startTime=" + startTime +
                ", finishedStatus=" + finishedStatus +
                ", expiryDate=" + expiryDate +
                '}';
    }

    public static final class Builder {
        private String taskId;
        private Instant startTime;
        private CompactionTaskFinishedStatus finishedStatus;
        private Instant expiryDate;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder startTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder finishedStatus(CompactionTaskFinishedStatus finishedStatus) {
            this.finishedStatus = finishedStatus;
            return this;
        }

        public Builder expiryDate(Instant expiryDate) {
            this.expiryDate = expiryDate;
            return this;
        }

        public Builder finished(Instant finishTime, CompactionTaskFinishedStatus.Builder taskFinishedBuilder) {
            return finishedStatus(taskFinishedBuilder
                    .finish(finishTime)
                    .build());
        }

        public String getTaskId() {
            return this.taskId;
        }

        public CompactionTaskStatus build() {
            return new CompactionTaskStatus(this);
        }
    }
}
