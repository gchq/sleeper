/*
 * Copyright 2022-2025 Crown Copyright
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

package sleeper.core.tracker.compaction.job.query;

import sleeper.core.tracker.job.status.JobRunStartedUpdate;

import java.time.Instant;
import java.util.Objects;

/**
 * A status update for when a compaction job has been picked up by a task and started. This is the model for querying
 * the event from the tracker.
 */
public class CompactionJobStartedStatus implements JobRunStartedUpdate {

    private final Instant updateTime;
    private final Instant startTime;

    private CompactionJobStartedStatus(Instant updateTime, Instant startTime) {
        this.updateTime = Objects.requireNonNull(updateTime, "updateTime may not be null");
        this.startTime = Objects.requireNonNull(startTime, "startTime may not be null");
    }

    /**
     * Creates a status update based on the time the job was received and when it was tracked.
     *
     * @param  startTime  the time the compaction job was started in the task
     * @param  updateTime the time the event was added to the tracker
     * @return            the status update
     */
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
