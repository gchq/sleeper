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
package sleeper.systemtest.dsl.statestore;

import sleeper.compaction.job.commit.CompactionJobIdAssignmentCommitRequest;
import sleeper.compaction.job.commit.CompactionJobIdAssignmentCommitRequestSerDe;
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequestSerDe;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

public class StateStoreCommitMessageFactory {

    private final String tableId;

    public StateStoreCommitMessageFactory(String tableId) {
        this.tableId = tableId;
    }

    public StateStoreCommitMessage addFiles(List<FileReference> files) {
        return ingest(builder -> builder.fileReferences(files));
    }

    public StateStoreCommitMessage addFileWithJob(FileReference file) {
        String jobId = UUID.randomUUID().toString();
        IngestJob job = IngestJob.builder()
                .id(jobId)
                .tableId(tableId)
                .files(List.of("test-file-" + jobId + ".parquet"))
                .build();
        return ingest(builder -> builder
                .fileReferences(List.of(file))
                .ingestJob(job)
                .taskId(jobId)
                .jobRunId(jobId)
                .writtenTime(Instant.now()));
    }

    public StateStoreCommitMessage assignJobOnPartitionToFiles(String jobId, String partitionId, List<String> filenames) {
        CompactionJobIdAssignmentCommitRequest request = new CompactionJobIdAssignmentCommitRequest(
                List.of(AssignJobIdRequest.assignJobOnPartitionToFiles(jobId, partitionId, filenames)), tableId);
        return StateStoreCommitMessage.tableIdAndBody(tableId,
                new CompactionJobIdAssignmentCommitRequestSerDe().toJson(request));
    }

    private StateStoreCommitMessage ingest(Consumer<IngestAddFilesCommitRequest.Builder> config) {
        IngestAddFilesCommitRequest.Builder builder = IngestAddFilesCommitRequest.builder().tableId(tableId);
        config.accept(builder);
        return StateStoreCommitMessage.tableIdAndBody(tableId,
                new IngestAddFilesCommitRequestSerDe().toJson(builder.build()));
    }

}
