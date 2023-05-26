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

import java.time.Instant;
import java.util.Objects;

public class IngestJobRejectedStatus implements IngestJobValidatedStatus {
    private final Instant updateTime;
    private final String reason;

    private IngestJobRejectedStatus(Builder builder) {
        updateTime = builder.updateTime;
        reason = builder.reason;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Instant getUpdateTime() {
        return updateTime;
    }

    @Override
    public Instant getStartTime() {
        return updateTime;
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
        return Objects.equals(updateTime, that.updateTime) && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, reason);
    }

    @Override
    public String toString() {
        return "IngestJobRejectedStatus{" +
                "updateTime=" + updateTime +
                ", reason='" + reason + '\'' +
                '}';
    }

    public static final class Builder {
        private Instant updateTime;
        private String reason;

        private Builder() {
        }

        public Builder validationTime(Instant updateTime) {
            this.updateTime = updateTime;
            return this;
        }

        public Builder reason(String reason) {
            this.reason = reason;
            return this;
        }

        public IngestJobRejectedStatus build() {
            return new IngestJobRejectedStatus(this);
        }
    }
}
