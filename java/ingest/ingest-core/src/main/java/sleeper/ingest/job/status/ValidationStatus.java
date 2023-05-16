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

import sleeper.core.record.process.status.ProcessStatusUpdate;

import java.time.Instant;
import java.util.Objects;

public class ValidationStatus implements ProcessStatusUpdate {
    private final Instant updateTime;
    private final ValidationData validationData;

    private ValidationStatus(Builder builder) {
        updateTime = builder.updateTime;
        validationData = builder.validationData;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Instant getUpdateTime() {
        return updateTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ValidationStatus that = (ValidationStatus) o;
        return Objects.equals(updateTime, that.updateTime)
                && Objects.equals(validationData, that.validationData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, validationData);
    }

    @Override
    public String toString() {
        return "ValidationStatus{" +
                "updateTime=" + updateTime +
                ", validationData=" + validationData +
                '}';
    }

    @Override
    public boolean isStartOfRun() {
        return true;
    }

    public static final class Builder {
        private Instant updateTime;
        private ValidationData validationData;

        public Builder() {
        }

        public Builder updateTime(Instant updateTime) {
            this.updateTime = updateTime;
            return this;
        }

        public Builder validationData(ValidationData validationData) {
            this.validationData = validationData;
            return this;
        }

        public ValidationStatus build() {
            return new ValidationStatus(this);
        }
    }
}
