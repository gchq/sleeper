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

package sleeper.compaction.core.job.query;

import sleeper.core.record.process.status.ProcessRunStartedUpdate;

import java.time.Instant;
import java.util.Objects;

public class CompactionJobStartedStatus implements ProcessRunStartedUpdate {

    private final Instant updateTime;
    private final Instant startTime;

    private CompactionJobStartedStatus(Instant updateTime, Instant startTime) {
        this.updateTime = Objects.requireNonNull(updateTime, "updateTime may not be null");
        this.startTime = Objects.requireNonNull(startTime, "startTime may not be null");
    }

    public static CompactionJobStartedStatus startAndUpdateTime(Instant startTime, Instant updateTime) {
        return new CompactionJobStartedStatus(updateTime, startTime);
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactionJobStartedStatus that = (CompactionJobStartedStatus) o;
        return updateTime.equals(that.updateTime) && startTime.equals(that.startTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, startTime);
    }

    @Override
    public String toString() {
        return "CompactionJobStartedStatus{" +
                "updateTime=" + updateTime +
                ", startTime=" + startTime +
                '}';
    }
}
