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

package sleeper.ingest.job.status;

import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessRunFinishedUpdate;
import sleeper.ingest.job.IngestJob;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * An ingest job validation status for when the job has failed validation.
 */
public class IngestJobRejectedStatus implements IngestJobValidatedStatus, ProcessRunFinishedUpdate {
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

    public String getJsonMessage() {
        return jsonMessage;
    }

    @Override
    public boolean isValid() {
        return false;
    }

    @Override
    public RecordsProcessedSummary getSummary() {
        return RecordsProcessedSummary.noProcessingDoneAtTime(validationTime);
    }

    @Override
    public boolean isSuccessful() {
        return false;
    }

    @Override
    public List<String> getFailureReasons() {
        return reasons;
    }

    @Override
    public boolean isPartOfRun() {
        return true;
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
                ", jsonMessage=\"" + jsonMessage + "\"" +
                '}';
    }

    /**
     * Builder for ingest job rejected status objects.
     */
    public static final class Builder {
        private Instant updateTime;
        private int inputFileCount = 0;
        private List<String> reasons;
        private String jsonMessage;
        private Instant validationTime;

        private Builder() {
        }

        /**
         * Sets the validation time.
         *
         * @param  validationTime the validation time
         * @return                the builder
         */
        public Builder validationTime(Instant validationTime) {
            this.validationTime = validationTime;
            return this;
        }

        /**
         * Sets the update time.
         *
         * @param  updateTime the update time
         * @return            the builder
         */
        public Builder updateTime(Instant updateTime) {
            this.updateTime = updateTime;
            return this;
        }

        /**
         * Sets the reasons for why the job was rejected.
         *
         * @param  reasons the list of reasons
         * @return         the builder
         */
        public Builder reasons(List<String> reasons) {
            this.reasons = reasons;
            return this;
        }

        /**
         * Sets the JSON message.
         *
         * @param  jsonMessage the JSON message
         * @return             the builder
         */
        public Builder jsonMessage(String jsonMessage) {
            this.jsonMessage = jsonMessage;
            return this;
        }

        /**
         * Sets the input file count.
         *
         * @param  inputFileCount the input file count
         * @return                the builder
         */
        public Builder inputFileCount(int inputFileCount) {
            this.inputFileCount = inputFileCount;
            return this;
        }

        /**
         * Sets the input file count using the ingest job.
         *
         * @param  job the ingest job
         * @return     the builder
         */
        public Builder job(IngestJob job) {
            return inputFileCount(job.getFileCount());
        }

        public IngestJobRejectedStatus build() {
            return new IngestJobRejectedStatus(this);
        }
    }
}
