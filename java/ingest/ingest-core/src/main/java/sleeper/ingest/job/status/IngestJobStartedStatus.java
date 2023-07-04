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
package sleeper.ingest.job.status;

import sleeper.ingest.job.IngestJob;

import java.time.Instant;
import java.util.Objects;

public class IngestJobStartedStatus implements IngestJobInfoStatus {

    private final int inputFileCount;
    private final Instant startTime;
    private final Instant updateTime;
    private final boolean isStartOfRun;

    private IngestJobStartedStatus(Builder builder) {
        inputFileCount = builder.inputFileCount;
        startTime = Objects.requireNonNull(builder.startTime, "startTime may not be null");
        updateTime = Objects.requireNonNull(builder.updateTime, "updateTime may not be null");
        isStartOfRun = builder.isStartOfRun;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder withStartOfRun(boolean startOfRun) {
        return builder().isStartOfRun(startOfRun);
    }

    public static IngestJobStartedStatus startAndUpdateTime(IngestJob job, Instant startTime, Instant updateTime) {
        return withStartOfRun(true).job(job)
                .startTime(startTime).updateTime(updateTime)
                .build();
    }

    public int getInputFileCount() {
        return inputFileCount;
    }

    @Override
    public Instant getUpdateTime() {
        return updateTime;
    }

    @Override
    public Instant getStartTime() {
        return startTime;
    }

    @Override
    public boolean isStartOfRun() {
        return isStartOfRun;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IngestJobStartedStatus that = (IngestJobStartedStatus) o;
        return inputFileCount == that.inputFileCount
                && isStartOfRun == that.isStartOfRun
                && Objects.equals(startTime, that.startTime)
                && Objects.equals(updateTime, that.updateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputFileCount, startTime, updateTime, isStartOfRun);
    }

    @Override
    public String toString() {
        return "IngestJobStartedStatus{" +
                "inputFileCount=" + inputFileCount +
                ", startTime=" + startTime +
                ", updateTime=" + updateTime +
                ", isStartOfRun=" + isStartOfRun +
                '}';
    }

    public static final class Builder {
        private int inputFileCount;
        private Instant startTime;
        private Instant updateTime;
        private boolean isStartOfRun = true;

        public Builder() {
        }

        public Builder job(IngestJob job) {
            return inputFileCount(job.getFiles().size());
        }

        public Builder inputFileCount(int inputFileCount) {
            this.inputFileCount = inputFileCount;
            return this;
        }

        public Builder startTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder updateTime(Instant updateTime) {
            this.updateTime = updateTime;
            return this;
        }

        public Builder isStartOfRun(boolean isStandardIngest) {
            this.isStartOfRun = isStandardIngest;
            return this;
        }

        public IngestJobStartedStatus build() {
            return new IngestJobStartedStatus(this);
        }
    }
}
