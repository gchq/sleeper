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

import java.util.Objects;

public class ValidationData {
    private final boolean valid;
    private final String reason;

    private ValidationData(Builder builder) {
        valid = builder.valid;
        reason = builder.reason;
    }

    public static ValidationData valid() {
        return new Builder().valid(true).build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ValidationData that = (ValidationData) o;
        return valid == that.valid && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(valid, reason);
    }

    @Override
    public String toString() {
        return "ValidationData{" +
                "valid=" + valid +
                ", reason='" + reason + '\'' +
                '}';
    }

    public static final class Builder {
        private boolean valid;
        private final String reason = "";

        public Builder() {
        }

        public Builder valid(boolean valid) {
            this.valid = valid;
            return this;
        }

        public ValidationData build() {
            return new ValidationData(this);
        }
    }
}
