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

import sleeper.compaction.job.status.CompactionJobStatusUpdate;
import sleeper.compaction.job.status.CompactionJobStatusUpdateRecord;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestCompactionJobStatusUpdateRecords {

    private final List<CompactionJobStatusUpdateRecord> records = new ArrayList<>();

    public TestCompactionJobStatusUpdateRecords recordsForJob(
            String jobId, CompactionJobStatusUpdate... statusUpdates) {
        Arrays.stream(statusUpdates)
                .map(update -> new CompactionJobStatusUpdateRecord(jobId, Instant.now(), update))
                .forEach(records::add);
        return this;
    }

    public List<CompactionJobStatusUpdateRecord> list() {
        return records;
    }
}
