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
package sleeper.compaction.core.job.status;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.core.record.process.status.ProcessStatusUpdate;

import java.time.Instant;
import java.util.Objects;

public class CompactionJobCreatedStatus implements ProcessStatusUpdate {

    private final Instant updateTime;
    private final String partitionId;
    private final int inputFilesCount;

    private CompactionJobCreatedStatus(Builder builder) {
        updateTime = Objects.requireNonNull(builder.updateTime, "updateTime must not be null");
        partitionId = Objects.requireNonNull(builder.partitionId, "partitionId must not be null");
        inputFilesCount = builder.inputFilesCount;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static CompactionJobCreatedStatus from(CompactionJob job, Instant updateTime) {
        return builder()
                .updateTime(updateTime)
                .partitionId(job.getPartitionId())
                .inputFilesCount(job.getInputFiles().size())
                .build();
    }

    public Instant getUpdateTime() {
        return updateTime;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public int getInputFilesCount() {
        return inputFilesCount;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactionJobCreatedStatus that = (CompactionJobCreatedStatus) o;
        return inputFilesCount == that.inputFilesCount
                && updateTime.equals(that.updateTime)
                && partitionId.equals(that.partitionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, partitionId, inputFilesCount);
    }

    @Override
    public String toString() {
        return "CompactionJobCreatedStatus{" +
                "updateTime=" + updateTime +
                ", partitionId='" + partitionId + '\'' +
                ", inputFilesCount=" + inputFilesCount +
                '}';
    }
}
