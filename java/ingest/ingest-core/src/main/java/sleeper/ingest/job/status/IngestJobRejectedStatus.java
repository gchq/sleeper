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
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class IngestJobRejectedStatus implements IngestJobValidatedStatus {
    private final Instant validationTime;
    private final Instant updateTime;
    private final int inputFileCount;
    private final List<String> reasons;
    private final String jsonMessage;

    private IngestJobRejectedStatus(Builder builder) {
        validationTime = Objects.requireNonNull(builder.validationTime, "validateTime must not be null");
        updateTime = Objects.requireNonNull(builder.updateTime, "updateTime must not be null");
        inputFileCount = builder.inputFileCount;
        reasons = Objects.requireNonNull(builder.reasons, "reasons must not be null");
        jsonMessage = builder.jsonMessage;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Instant getStartTime() {
        return validationTime;
    }

    @Override
    public Instant getUpdateTime() {
        return updateTime;
    }

    @Override
    public int getInputFileCount() {
        return inputFileCount;
    }

    public List<String> getReasons() {
        return reasons;
    }

    @Override
    public boolean isValid() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IngestJobRejectedStatus that = (IngestJobRejectedStatus) o;
        return inputFileCount == that.inputFileCount
                && Objects.equals(validationTime, that.validationTime)
                && Objects.equals(updateTime, that.updateTime)
                && Objects.equals(reasons, that.reasons)
                && Objects.equals(jsonMessage, that.jsonMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(validationTime, updateTime, inputFileCount, reasons, jsonMessage);
    }

    @Override
    public String toString() {
        return "IngestJobRejectedStatus{" +
                "validationTime=" + validationTime +
                ", updateTime=" + updateTime +
                ", inputFileCount=" + inputFileCount +
                ", reasons=" + reasons +
                ", jsonMessage=" + jsonMessage +
                '}';
    }

    public static final class Builder {
        private Instant updateTime;
        private int inputFileCount = 0;
        private List<String> reasons;
        private String jsonMessage;
        private Instant validationTime;

        private Builder() {
        }

        public Builder validationTime(Instant validationTime) {
            this.validationTime = validationTime;
            return this;
        }

        public Builder updateTime(Instant updateTime) {
            this.updateTime = updateTime;
            return this;
        }

        public Builder reasons(List<String> reasons) {
            this.reasons = reasons;
            return this;
        }

        public Builder jsonMessage(String jsonMessage) {
            this.jsonMessage = jsonMessage;
            return this;
        }

        public Builder inputFileCount(int inputFileCount) {
            this.inputFileCount = inputFileCount;
            return this;
        }

        public Builder job(IngestJob job) {
            return inputFileCount(Optional.ofNullable(job.getFiles()).map(List::size).orElse(0));
        }

        public IngestJobRejectedStatus build() {
            return new IngestJobRejectedStatus(this);
        }
    }
}
