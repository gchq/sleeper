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
package sleeper.compaction.core.job.status;

import sleeper.compaction.core.job.CompactionJob;

public class CompactionJobCreatedEvent {

    private final String jobId;
    private final String tableId;
    private final String partitionId;
    private final int inputFilesCount;

    private CompactionJobCreatedEvent(CompactionJob compactionJob) {
        jobId = compactionJob.getId();
        tableId = compactionJob.getTableId();
        partitionId = compactionJob.getPartitionId();
        inputFilesCount = compactionJob.getInputFiles().size();
    }

    public static CompactionJobCreatedEvent compactionJobCreated(CompactionJob compactionJob) {
        return new CompactionJobCreatedEvent(compactionJob);
    }

    public String getJobId() {
        return jobId;
    }

    public String getTableId() {
        return tableId;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public int getInputFilesCount() {
        return inputFilesCount;
    }
}
