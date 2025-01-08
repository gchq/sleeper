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
package sleeper.core.tracker.compaction.job.query;

import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.tracker.compaction.job.update.CompactionJobCreatedEvent;
import sleeper.core.tracker.job.status.ProcessStatusUpdate;

import java.time.Instant;
import java.util.Objects;

public class CompactionJobCreatedStatus implements ProcessStatusUpdate {

    private final Instant updateTime;
    private final String partitionId;
    private final int inputFilesCount;

    public CompactionJobCreatedStatus(Builder builder) {
        updateTime = builder.updateTime;
        partitionId = builder.partitionId;
        inputFilesCount = builder.inputFilesCount;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static CompactionJobCreatedStatus from(CompactionJobCreatedEvent event, Instant updateTime) {
        return builder()
                .updateTime(updateTime)
                .partitionId(event.getPartitionId())
                .inputFilesCount(event.getInputFilesCount())
                .build();
    }

    public static CompactionJobCreatedStatus from(AssignJobIdRequest request, Instant updateTime) {
        return builder()
                .updateTime(updateTime)
                .partitionId(request.getPartitionId())
                .inputFilesCount(request.getFilenames().size())
                .build();
    }

    @Override
    public Instant getUpdateTime() {
        return updateTime;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public int getInputFilesCount() {
        return inputFilesCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, partitionId, inputFilesCount);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CompactionJobCreatedStatus)) {
            return false;
        }
        CompactionJobCreatedStatus other = (CompactionJobCreatedStatus) obj;
        return Objects.equals(updateTime, other.updateTime) && Objects.equals(partitionId, other.partitionId) && inputFilesCount == other.inputFilesCount;
    }

    @Override
    public String toString() {
        return "CompactionJobCreatedStatus{updateTime=" + updateTime + ", partitionId=" + partitionId + ", inputFilesCount=" + inputFilesCount + "}";
    }

    public static final class Builder {
        private Instant updateTime;
        private String partitionId;
        private int inputFilesCount;

        private Builder() {
        }

        public Builder updateTime(Instant updateTime) {
            this.updateTime = updateTime;
            return this;
        }

        public Builder partitionId(String partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        public Builder inputFilesCount(int inputFilesCount) {
            this.inputFilesCount = inputFilesCount;
            return this;
        }

        public CompactionJobCreatedStatus build() {
            return new CompactionJobCreatedStatus(this);
        }
    }
}
