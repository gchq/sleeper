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

public class CompactionJobFinishedStatus {

    private final Instant updateTime;
    private final CompactionJobSummary summary;

    private CompactionJobFinishedStatus(Instant updateTime, CompactionJobSummary summary) {
        this.updateTime = updateTime;
        this.summary = summary;
    }

    public static CompactionJobFinishedStatus updateTimeAndSummary(Instant updateTime, CompactionJobSummary summary) {
        return new CompactionJobFinishedStatus(updateTime, summary);
    }

    public Instant getUpdateTime() {
        return updateTime;
    }

    public CompactionJobSummary getSummary() {
        return summary;
    }
}
