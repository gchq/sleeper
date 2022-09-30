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

package sleeper.compaction.task;

import java.time.Instant;
import java.util.Objects;

public class CompactionTaskStartedStatus {
    private final Instant startTime;
    private final Instant startUpdateTime;

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getStartUpdateTime() {
        return startUpdateTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactionTaskStartedStatus that = (CompactionTaskStartedStatus) o;
        return Objects.equals(startTime, that.startTime) && Objects.equals(startUpdateTime, that.startUpdateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTime, startUpdateTime);
    }

    @Override
    public String toString() {
        return "CompactionTaskStartedStatus{" +
                "startTime=" + startTime +
                ", startUpdateTime=" + startUpdateTime +
                '}';
    }

    private CompactionTaskStartedStatus(Builder builder) {
        startTime = builder.startTime;
        startUpdateTime = builder.startUpdateTime;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Instant startTime;
        private Instant startUpdateTime;

        private Builder() {
        }

        public Builder startTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder startUpdateTime(Instant startUpdateTime) {
            this.startUpdateTime = startUpdateTime;
            return this;
        }

        public CompactionTaskStartedStatus build() {
            return new CompactionTaskStartedStatus(this);
        }
    }
}
