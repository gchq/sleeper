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

import sleeper.compaction.job.CompactionJobSummary;

import java.time.Instant;
import java.util.Objects;

public class CompactionJobFinishedStatus {

    private final Instant updateTime;
    private final CompactionJobSummary summary;
    private final String taskId;

    private CompactionJobFinishedStatus(Instant updateTime, CompactionJobSummary summary, String taskId) {
        this.updateTime = Objects.requireNonNull(updateTime, "updateTime must not be null");
        this.summary = Objects.requireNonNull(summary, "summary must not be null");
        this.taskId = Objects.requireNonNull(taskId, "taskId must not be null");
    }

    public static CompactionJobFinishedStatus updateTimeAndSummaryWithTaskId(Instant updateTime, CompactionJobSummary summary, String taskId) {
        return new CompactionJobFinishedStatus(updateTime, summary, taskId);
    }

    public Instant getUpdateTime() {
        return updateTime;
    }

    public CompactionJobSummary getSummary() {
        return summary;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactionJobFinishedStatus that = (CompactionJobFinishedStatus) o;
        return updateTime.equals(that.updateTime) && summary.equals(that.summary) && taskId.equals(that.taskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, summary, taskId);
    }

    @Override
    public String toString() {
        return "CompactionJobFinishedStatus{" +
                "updateTime=" + updateTime +
                ", summary=" + summary +
                ", taskId='" + taskId + '\'' +
                '}';
    }
}
