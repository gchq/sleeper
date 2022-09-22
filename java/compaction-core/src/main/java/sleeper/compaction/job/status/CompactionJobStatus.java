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

import sleeper.compaction.job.CompactionJob;

import java.time.Instant;
import java.util.Objects;
import java.util.function.Consumer;

public class CompactionJobStatus {

    private final String jobId;
    private final CompactionJobCreatedStatus createdStatus;

    private CompactionJobStatus(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        createdStatus = Objects.requireNonNull(builder.createdStatus, "createdStatus must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static CompactionJobStatus created(CompactionJob job, Instant updateTime) {
        return builder()
                .jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, updateTime))
                .build();
    }

    public static final class Builder {
        private String jobId;
        private CompactionJobCreatedStatus createdStatus;

        private Builder() {
        }

        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder createdStatus(CompactionJobCreatedStatus createdStatus) {
            this.createdStatus = createdStatus;
            return this;
        }

        public Builder createdStatus(Consumer<CompactionJobCreatedStatus.Builder> config) {
            CompactionJobCreatedStatus.Builder builder = CompactionJobCreatedStatus.builder();
            config.accept(builder);
            return createdStatus(builder.build());
        }

        public CompactionJobStatus build() {
            return new CompactionJobStatus(this);
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
        CompactionJobStatus that = (CompactionJobStatus) o;
        return jobId.equals(that.jobId) && createdStatus.equals(that.createdStatus);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, createdStatus);
    }

    @Override
    public String toString() {
        return "CompactionJobStatus{" +
                "jobId='" + jobId + '\'' +
                ", createdStatus=" + createdStatus +
                '}';
    }
}
