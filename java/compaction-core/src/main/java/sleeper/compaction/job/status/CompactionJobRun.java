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

package sleeper.compaction.job.status;

import sleeper.compaction.job.CompactionJobSummary;

import java.time.Instant;
import java.util.Objects;

public class CompactionJobRun {
    private final String taskId;
    private final CompactionJobStartedStatus startedStatus;
    private final CompactionJobFinishedStatus finishedStatus;

    private CompactionJobRun(Builder builder) {
        taskId = Objects.requireNonNull(builder.taskId, "taskId must not be null");
        startedStatus = Objects.requireNonNull(builder.startedStatus, "startedStatus must not be null");
        finishedStatus = builder.finishedStatus;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static CompactionJobRun started(String taskId, CompactionJobStartedStatus startedStatus) {
        return builder().taskId(taskId)
                .startedStatus(startedStatus)
                .build();
    }

    public static CompactionJobRun finished(String taskId, CompactionJobStartedStatus startedStatus, CompactionJobFinishedStatus finishedStatus) {
        return builder().taskId(taskId)
                .startedStatus(startedStatus)
                .finishedStatus(finishedStatus)
                .build();
    }

    public String getTaskId() {
        return taskId;
    }

    public CompactionJobStartedStatus getStartedStatus() {
        return startedStatus;
    }

    public CompactionJobFinishedStatus getFinishedStatus() {
        return finishedStatus;
    }

    public boolean isFinished() {
        return finishedStatus != null;
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

    public CompactionJobSummary getFinishedSummary() {
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
        CompactionJobRun that = (CompactionJobRun) o;
        return taskId.equals(that.taskId)
                && Objects.equals(startedStatus, that.startedStatus)
                && Objects.equals(finishedStatus, that.finishedStatus);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, startedStatus, finishedStatus);
    }

    @Override
    public String toString() {
        return "CompactionJobRun{" +
                "taskId='" + taskId + '\'' +
                ", startedStatus=" + startedStatus +
                ", finishedStatus=" + finishedStatus +
                '}';
    }

    public static final class Builder {
        private String taskId;
        private CompactionJobStartedStatus startedStatus;
        private CompactionJobFinishedStatus finishedStatus;

        private Builder() {
        }

        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder startedStatus(CompactionJobStartedStatus startedStatus) {
            this.startedStatus = startedStatus;
            return this;
        }

        public Builder finishedStatus(CompactionJobFinishedStatus finishedStatus) {
            this.finishedStatus = finishedStatus;
            return this;
        }

        public CompactionJobRun build() {
            return new CompactionJobRun(this);
        }
    }
}
