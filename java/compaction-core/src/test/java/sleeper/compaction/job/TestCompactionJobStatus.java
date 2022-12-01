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
package sleeper.compaction.job;

import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.record.process.status.ProcessStatusUpdate;
import sleeper.core.record.process.status.TestProcessStatusUpdateRecords;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.records;

public class TestCompactionJobStatus {

    private TestCompactionJobStatus() {
    }

    public static CompactionJobStatus statusFromUpdates(ProcessStatusUpdate... updates) {
        return statusFrom(records().fromUpdates(updates));
    }

    public static List<CompactionJobStatus> statusListFromUpdates(
            TestProcessStatusUpdateRecords.TaskUpdates... updates) {
        return CompactionJobStatus.listFrom(records().fromUpdates(updates).stream());
    }

    private static CompactionJobStatus statusFrom(TestProcessStatusUpdateRecords records) {
        List<CompactionJobStatus> built = CompactionJobStatus.listFrom(records.stream());
        if (built.size() != 1) {
            throw new IllegalStateException("Expected single status");
        }
        return built.get(0);
    }

    public static CompactionJobStatus created(CompactionJob job, Instant updateTime) {
        return CompactionJobStatus.builder()
                .jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, updateTime))
                .jobRunsLatestFirst(Collections.emptyList())
                .build();
    }
}
