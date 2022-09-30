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

package sleeper.compaction.task;

import sleeper.compaction.job.status.CompactionJobStatus;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class CompactionTaskStatus {
    private final String taskId;
    private final CompactionTaskStartedStatus startedStatus;
    private CompactionTaskFinishedStatus finishedStatus;
    private final Instant expiryDate;

    private CompactionTaskStatus(Builder builder) {
        taskId = builder.taskId;
        startedStatus = builder.startedStatus;
        finishedStatus = builder.finishedStatus;
        expiryDate = builder.expiryDate;
    }

    public static CompactionTaskStatus started(long startTime) {
        return builder().taskId(UUID.randomUUID().toString()).startedStatus(
                        CompactionTaskStartedStatus.builder()
                                .startTime(Instant.ofEpochMilli(startTime))
                                .startUpdateTime(Instant.ofEpochMilli(startTime))
                                .build())
                .build();
    }

    public void finished(List<CompactionJobStatus> jobStatusList, long finishTime) {
        this.finishedStatus = CompactionTaskFinishedStatus.fromJobList(jobStatusList,
                Instant.ofEpochMilli(finishTime), (finishTime - startedStatus.getStartTime().toEpochMilli()) / 1000.0);
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getTaskId() {
        return taskId;
    }

    public CompactionTaskStartedStatus getStartedStatus() {
        return startedStatus;
    }

    public CompactionTaskFinishedStatus getFinishedStatus() {
        return finishedStatus;
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
        return Objects.equals(taskId, that.taskId) && Objects.equals(startedStatus, that.startedStatus) && Objects.equals(finishedStatus, that.finishedStatus);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, startedStatus, finishedStatus);
    }

    @Override
    public String toString() {
        return "CompactionTaskStatus{" +
                "taskId='" + taskId + '\'' +
                ", startedStatus=" + startedStatus +
                ", finishedStatus=" + finishedStatus +
                '}';
    }

    public static final class Builder {
        private String taskId;
        private CompactionTaskStartedStatus startedStatus;
        private CompactionTaskFinishedStatus finishedStatus;
        private Instant expiryDate;

        private Builder() {
        }

        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder startedStatus(CompactionTaskStartedStatus startedStatus) {
            this.startedStatus = startedStatus;
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

        public CompactionTaskStatus build() {
            return new CompactionTaskStatus(this);
        }
    }
}
