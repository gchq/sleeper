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
package sleeper.compaction.job.status;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public class CompactionJobStartedStatus {

    private final Instant updateTime;
    private final Instant startTime;
    private final String taskId;

    private CompactionJobStartedStatus(Instant updateTime, Instant startTime, String taskId) {
        this.updateTime = Objects.requireNonNull(updateTime, "updateTime may not be null");
        this.startTime = Objects.requireNonNull(startTime, "startTime may not be null");
        this.taskId = taskId;
    }

    public static CompactionJobStartedStatus updateAndStartTimeWithTaskId(Instant updateTime, Instant startTime, String taskId) {
        return new CompactionJobStartedStatus(updateTime, startTime, taskId);
    }

    public static CompactionJobStartedStatus updateAndStartTime(Instant updateTime, Instant startTime) {
        return new CompactionJobStartedStatus(updateTime, startTime, UUID.randomUUID().toString());
    }

    public Instant getUpdateTime() {
        return updateTime;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public String getTaskId() {
        return taskId;
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
        return updateTime.equals(that.updateTime) && startTime.equals(that.startTime) && taskId.equals(that.taskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, startTime, taskId);
    }

    @Override
    public String toString() {
        return "CompactionJobStartedStatus{" +
                "updateTime=" + updateTime +
                ", startTime=" + startTime +
                ", taskId='" + taskId + "'" +
                '}';
    }
}
