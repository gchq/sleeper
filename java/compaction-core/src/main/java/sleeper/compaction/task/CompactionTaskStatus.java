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
    private final Instant creationTime;
    private CompactionTaskFinishedStatus finishedStatus;

    private CompactionTaskStatus(Builder builder) {
        taskId = builder.taskId;
        creationTime = builder.creationTime;
        finishedStatus = builder.finishedStatus;
    }

    public static CompactionTaskStatus created(long creationTime) {
        return builder().taskId(UUID.randomUUID().toString()).creationTime(Instant.ofEpochMilli(creationTime))
                .build();
    }

    public void finished(List<CompactionJobStatus> jobStatusList, long finishTime) {
        this.finishedStatus = CompactionTaskFinishedStatus.fromJobList(jobStatusList,
                Instant.ofEpochMilli(finishTime), (finishTime - creationTime.toEpochMilli()) / 1000.0);
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getTaskId() {
        return taskId;
    }

    public Instant getCreationTime() {
        return creationTime;
    }

    public CompactionTaskFinishedStatus getFinishedStatus() {
        return finishedStatus;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompactionTaskStatus that = (CompactionTaskStatus) o;
        return Objects.equals(taskId, that.taskId) && Objects.equals(creationTime, that.creationTime) && Objects.equals(finishedStatus, that.finishedStatus);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, creationTime, finishedStatus);
    }

    @Override
    public String toString() {
        return "CompactionTaskStatus{" +
                "taskId='" + taskId + '\'' +
                ", creationTime=" + creationTime +
                ", finishedStatus=" + finishedStatus +
                '}';
    }

    public static final class Builder {
        private String taskId;
        private Instant creationTime;
        private CompactionTaskFinishedStatus finishedStatus;

        private Builder() {
        }

        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder creationTime(Instant creationTime) {
            this.creationTime = creationTime;
            return this;
        }

        public Builder finishedStatus(CompactionTaskFinishedStatus finishedStatus) {
            this.finishedStatus = finishedStatus;
            return this;
        }

        public CompactionTaskStatus build() {
            return new CompactionTaskStatus(this);
        }
    }
}
