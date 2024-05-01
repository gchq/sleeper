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
package sleeper.compaction.job;

import sleeper.core.record.process.RecordsProcessed;

import java.time.Instant;

public class CompactionJobCompletionRequest {

    private final CompactionJob job;
    private final String taskId;
    private final Instant startTime;
    private final RecordsProcessed recordsProcessed;

    public CompactionJobCompletionRequest(
            CompactionJob job, String taskId, Instant startTime, RecordsProcessed recordsProcessed) {
        this.job = job;
        this.taskId = taskId;
        this.startTime = startTime;
        this.recordsProcessed = recordsProcessed;
    }

    public CompactionJob getJob() {
        return job;
    }

    public String getTaskId() {
        return taskId;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public RecordsProcessed getRecordsProcessed() {
        return recordsProcessed;
    }

}
