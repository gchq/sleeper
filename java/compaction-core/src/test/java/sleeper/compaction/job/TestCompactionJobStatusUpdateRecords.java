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
import sleeper.core.record.process.status.ProcessStatusUpdateRecord;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class TestCompactionJobStatusUpdateRecords {

    private final List<ProcessStatusUpdateRecord> updates = new ArrayList<>();

    public TestCompactionJobStatusUpdateRecords updatesForJobWithTask(
            String jobId, String taskId, ProcessStatusUpdate... statusUpdates) {
        return forJob(jobId, records -> records.updatesWithTask(taskId, statusUpdates));
    }

    public TestCompactionJobStatusUpdateRecords jobCreated(
            String jobId, CompactionJobCreatedStatus created) {
        return updatesForJobWithTask(jobId, null, created);
    }

    public TestCompactionJobStatusUpdateRecords forJob(String jobId, Consumer<WithJob> config) {
        config.accept(new WithJob(jobId));
        return this;
    }

    public List<ProcessStatusUpdateRecord> list() {
        return updates;
    }

    public List<CompactionJobStatus> listCompactionJobs() {
        return CompactionJobStatus.listFrom(updates.stream());
    }

    public CompactionJobStatus buildSingleStatus() {
        List<CompactionJobStatus> built = listCompactionJobs();
        if (built.size() != 1) {
            throw new IllegalStateException("Expected single status");
        }
        return built.get(0);
    }

    public class WithJob {
        private final String jobId;
        private final Instant expiryDate = Instant.ofEpochSecond(999999999);

        private WithJob(String jobId) {
            this.jobId = jobId;
        }

        public WithJob updatesWithTask(String taskId, ProcessStatusUpdate... statusUpdates) {
            Arrays.stream(statusUpdates)
                    .map(update -> new ProcessStatusUpdateRecord(jobId, expiryDate, update, taskId))
                    .forEach(updates::add);
            return this;
        }
    }
}
