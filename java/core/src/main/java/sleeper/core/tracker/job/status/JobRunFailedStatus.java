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
package sleeper.core.tracker.job.status;

import sleeper.core.tracker.job.run.RecordsProcessed;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a job failing.
 */
public class JobRunFailedStatus implements JobRunEndUpdate {

    private final Instant updateTime;
    private final Instant failureTime;
    private final List<String> failureReasons;
    private final Duration timeInProcess;

    private JobRunFailedStatus(Builder builder) {
        updateTime = Objects.requireNonNull(builder.updateTime, "updateTime must not be null");
        failureTime = Objects.requireNonNull(builder.failureTime, "failureTime must not be null");
        failureReasons = Objects.requireNonNull(builder.failureReasons, "failureReasons must not be null");
        timeInProcess = builder.timeInProcess;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Instant getUpdateTime() {
        return updateTime;
    }

    @Override
    public Instant getFinishTime() {
        return failureTime;
    }

    @Override
    public Optional<Duration> getTimeInProcess() {
        return Optional.ofNullable(timeInProcess);
    }

    @Override
    public RecordsProcessed getRecordsProcessed() {
        return RecordsProcessed.NONE;
    }

    @Override
    public boolean isSuccessful() {
        return false;
    }

    @Override
    public List<String> getFailureReasons() {
        return failureReasons;
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, failureTime, failureReasons, timeInProcess);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof JobRunFailedStatus)) {
            return false;
        }
        JobRunFailedStatus other = (JobRunFailedStatus) obj;
        return Objects.equals(updateTime, other.updateTime) && Objects.equals(failureTime, other.failureTime) && Objects.equals(failureReasons, other.failureReasons)
                && Objects.equals(timeInProcess, other.timeInProcess);
    }

    @Override
    public String toString() {
        return "JobRunFailedStatus{updateTime=" + updateTime + ", failureTime=" + failureTime + ", failureReasons=" + failureReasons + ", timeInProcess=" + timeInProcess + "}";
    }

    /**
     * A builder for this class.
     */
    public static class Builder {
        private Instant updateTime;
        private Instant failureTime;
        private List<String> failureReasons;
        private Duration timeInProcess;

        private Builder() {
        }

        /**
         * Sets the update time.
         *
         * @param  updateTime the time
         * @return            the builder for chaining
         */
        public Builder updateTime(Instant updateTime) {
            this.updateTime = updateTime;
            return this;
        }

        /**
         * Sets the time of the failure.
         *
         * @param  failureTime the failure time
         * @return             the builder for chaining
         */
        public Builder failureTime(Instant failureTime) {
            this.failureTime = failureTime;
            return this;
        }

        /**
         * Sets the reasons the job failed.
         *
         * @param  failureReasons the reasons
         * @return                the builder for chaining
         */
        public Builder failureReasons(List<String> failureReasons) {
            this.failureReasons = failureReasons;
            return this;
        }

        /**
         * Sets the records processed.
         *
         * @param  timeInProcess the records processed
         * @return               the builder for chaining
         */
        public Builder timeInProcess(Duration timeInProcess) {
            this.timeInProcess = timeInProcess;
            return this;
        }

        public JobRunFailedStatus build() {
            return new JobRunFailedStatus(this);
        }
    }
}
