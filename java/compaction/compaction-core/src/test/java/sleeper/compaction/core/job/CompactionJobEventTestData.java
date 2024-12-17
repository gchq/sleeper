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
package sleeper.compaction.core.job;

import sleeper.core.tracker.compaction.job.update.CompactionJobCreatedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobStartedEvent;

import java.time.Instant;
import java.util.UUID;

public class CompactionJobEventTestData {

    private CompactionJobEventTestData() {
    }

    public static CompactionJobCreatedEvent defaultCompactionJobCreatedEvent() {
        return CompactionJobCreatedEvent.builder()
                .jobId(UUID.randomUUID().toString())
                .tableId("test-table")
                .partitionId("root")
                .inputFilesCount(1)
                .build();
    }

    public static CompactionJobStartedEvent.Builder startedEventBuilder(CompactionJobCreatedEvent created, Instant startTime) {
        return CompactionJobStartedEvent.builder().jobId(created.getJobId()).tableId(created.getTableId()).startTime(startTime);
    }

}
