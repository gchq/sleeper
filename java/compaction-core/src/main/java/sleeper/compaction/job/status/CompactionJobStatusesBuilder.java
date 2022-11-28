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

import sleeper.core.record.process.status.JobStatusUpdates;
import sleeper.core.record.process.status.JobStatusesBuilder;
import sleeper.core.record.process.status.ProcessStatusUpdateRecord;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CompactionJobStatusesBuilder {
    private final JobStatusesBuilder statusesBuilder = new JobStatusesBuilder();

    public CompactionJobStatusesBuilder jobUpdates(List<ProcessStatusUpdateRecord> jobUpdates) {
        jobUpdates.forEach(this::jobUpdate);
        return this;
    }

    public CompactionJobStatusesBuilder jobUpdate(ProcessStatusUpdateRecord jobUpdate) {
        statusesBuilder.update(jobUpdate);
        return this;
    }

    public Stream<CompactionJobStatus> stream() {
        return statusesBuilder.stream().map(this::fullStatus);
    }

    public List<CompactionJobStatus> build() {
        return stream().collect(Collectors.toList());
    }

    private CompactionJobStatus fullStatus(JobStatusUpdates job) {
        return CompactionJobStatus.builder().jobId(job.getJobId())
                .createdStatus((CompactionJobCreatedStatus) job.getFirstRecord().getStatusUpdate())
                .jobRuns(job.getRuns())
                .expiryDate(job.getLastRecord().getExpiryDate())
                .build();
    }
}
