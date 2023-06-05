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
import java.util.List;
import java.util.Objects;

public class IngestJobRejectedStatus implements IngestJobValidatedStatus {
    private final Instant updateTime;
    private final List<String> reasons;

    private IngestJobRejectedStatus(Builder builder) {
        updateTime = builder.updateTime;
        reasons = builder.reasons;
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
        return Objects.equals(updateTime, that.updateTime) && Objects.equals(reasons, that.reasons);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, reasons);
    }

    @Override
    public String toString() {
        return "IngestJobRejectedStatus{" +
                "updateTime=" + updateTime +
                ", reasons=" + reasons +
                '}';
    }

    public static final class Builder {
        private Instant updateTime;
        private List<String> reasons;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder validationTime(Instant updateTime) {
            this.updateTime = updateTime;
            return this;
        }

        public Builder reasons(List<String> reasons) {
            this.reasons = reasons;
            return this;
        }

        public IngestJobRejectedStatus build() {
            return new IngestJobRejectedStatus(this);
        }


    }
}
