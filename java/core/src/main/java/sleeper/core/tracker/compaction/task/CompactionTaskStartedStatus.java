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

package sleeper.core.tracker.compaction.task;

import sleeper.core.tracker.job.status.JobRunStartedUpdate;

import java.time.Instant;
import java.util.Objects;

public class CompactionTaskStartedStatus implements JobRunStartedUpdate {

    private final Instant startTime;

    private CompactionTaskStartedStatus(Instant startTime) {
        this.startTime = Objects.requireNonNull(startTime, "startTime may not be null");
    }

    public static CompactionTaskStartedStatus startTime(Instant startTime) {
        return new CompactionTaskStartedStatus(startTime);
    }

    @Override
    public Instant getUpdateTime() {
        return startTime;
    }

    @Override
    public Instant getStartTime() {
        return startTime;
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
        return startTime.equals(that.startTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTime);
    }

    @Override
    public String toString() {
        return "CompactionTaskStartedStatus{" +
                "startTime=" + startTime +
                '}';
    }
}
