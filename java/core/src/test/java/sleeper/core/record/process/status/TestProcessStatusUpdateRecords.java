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

package sleeper.core.record.process.status;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestProcessStatusUpdateRecords {
    private final List<ProcessStatusUpdateRecord> updates = new ArrayList<>();

    public TestProcessStatusUpdateRecords updatesForJobWithTask(
            String jobId, String taskId, ProcessStatusUpdate... statusUpdates) {
        return forJob(jobId, records -> records.updatesWithTask(taskId, statusUpdates));
    }

    public TestProcessStatusUpdateRecords forJob(String jobId, Consumer<WithJob> config) {
        config.accept(new WithJob(jobId));
        return this;
    }

    public ProcessRuns buildRuns() {
        List<JobStatusUpdates> built = JobStatusUpdates.streamFrom(updates.stream())
                .collect(Collectors.toList());
        if (built.size() != 1) {
            throw new IllegalStateException("Expected single status");
        }
        return built.get(0).getRuns();
    }

    public class WithJob {
        private final String jobId;
        private final Instant expiryDate = Instant.ofEpochSecond(999999999);

        private WithJob(String jobId) {
            this.jobId = jobId;
        }

        public WithJob updatesWithTask(String taskId, ProcessStatusUpdate... statusUpdates) {
            return updatesWithTask(taskId, Stream.of(statusUpdates));
        }

        public WithJob updatesWithTask(String taskId, Stream<ProcessStatusUpdate> statusUpdates) {
            statusUpdates.map(update -> new ProcessStatusUpdateRecord(jobId, expiryDate, update, taskId))
                    .forEach(updates::add);
            return this;
        }
    }
}
